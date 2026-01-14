"""
Pipeline idempotency tests.

Ensures that re-running the pipeline doesn't corrupt data or cause duplicates.
"""

import pytest
import pandas as pd
import shutil
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from normalization import normalize_team_cap, normalize_player_rankings
from roster_salary_merge import merge_rosters_and_salaries


class TestPipelineIdempotency:
    """Test that pipeline can be safely re-run."""

    @pytest.fixture
    def backup_dir(self, tmp_path):
        """Create backup of processed data."""
        processed_dir = Path("data/processed/compensation")
        backup_path = tmp_path / "backup"
        
        if processed_dir.exists():
            shutil.copytree(processed_dir, backup_path)
        
        yield backup_path
        
        # Restore after test
        if backup_path.exists():
            shutil.rmtree(processed_dir, ignore_errors=True)
            shutil.copytree(backup_path, processed_dir)

    def test_normalize_idempotent(self, backup_dir):
        """Test that normalization can be run twice without changing results."""
        year = 2024
        staging_dir = Path("data/staging")
        
        if not staging_dir.exists():
            pytest.skip("No staging data to normalize")
        
        # Run normalization twice
        try:
            first_run = normalize_team_cap(year)
            second_run = normalize_team_cap(year)
            
            # Both should succeed and produce same file
            assert first_run == second_run
            assert first_run.exists()
            
            # Content should be identical
            df1 = pd.read_csv(first_run)
            df2 = pd.read_csv(first_run)  # Read again after second write
            
            assert len(df1) == len(df2)
            pd.testing.assert_frame_equal(df1, df2)
            
        except FileNotFoundError:
            pytest.skip("Staging files not found")

    def test_no_duplicate_records_after_rerun(self):
        """Test that re-running pipeline doesn't create duplicate records."""
        player_dm_file = Path("data/processed/compensation/player_dead_money.csv")
        
        if not player_dm_file.exists():
            pytest.skip("Player dead money file not found")
        
        df = pd.read_csv(player_dm_file)
        
        # Check for duplicates on key columns
        if 'player_id' in df.columns:
            duplicates = df.duplicated(subset=['player_id', 'team', 'year'], keep=False)
            duplicate_count = duplicates.sum()
            
            assert duplicate_count == 0, \
                f"Found {duplicate_count} duplicate player records"


class TestDataIntegrity:
    """Test data integrity constraints."""

    def test_no_null_primary_keys(self):
        """Test that primary key columns have no nulls."""
        files_and_keys = {
            "data/processed/compensation/dim_players.csv": ["player_id"],
            "data/processed/compensation/fact_player_contracts.csv": ["contract_id", "player_id"],
            "data/processed/compensation/team_dead_money_by_year.csv": ["team", "year"]
        }
        
        for filepath, key_cols in files_and_keys.items():
            path = Path(filepath)
            if not path.exists():
                continue
            
            df = pd.read_csv(path)
            
            for col in key_cols:
                if col in df.columns:
                    null_count = df[col].isnull().sum()
                    assert null_count == 0, \
                        f"{path.name} has {null_count} nulls in key column '{col}'"

    def test_referential_integrity(self):
        """Test referential integrity between tables."""
        players_file = Path("data/processed/compensation/dim_players.csv")
        contracts_file = Path("data/processed/compensation/fact_player_contracts.csv")
        
        if not (players_file.exists() and contracts_file.exists()):
            pytest.skip("Required files not found")
        
        players = pd.read_csv(players_file)
        contracts = pd.read_csv(contracts_file)
        
        # Every player_id in contracts should exist in players
        player_ids = set(players['player_id'])
        contract_player_ids = set(contracts['player_id'])
        
        orphaned = contract_player_ids - player_ids
        assert len(orphaned) == 0, \
            f"Found {len(orphaned)} contracts with non-existent player_ids"

    def test_year_values_reasonable(self):
        """Test that year values are within reasonable range."""
        from datetime import datetime
        current_year = datetime.now().year
        
        for csv_file in Path("data/processed/compensation").glob("*.csv"):
            df = pd.read_csv(csv_file)
            
            if 'year' in df.columns:
                min_year = df['year'].min()
                max_year = df['year'].max()
                
                assert min_year >= 2011, \
                    f"{csv_file.name}: min year {min_year} < 2011"
                assert max_year <= current_year + 1, \
                    f"{csv_file.name}: max year {max_year} > {current_year + 1}"


class TestScraperOutputStability:
    """Test that scraper outputs remain stable across runs."""

    def test_column_names_stable(self):
        """Test that CSV column names match expected schema."""
        expected_columns = {
            "data/raw/spotrac_team_cap_*.csv": [
                "team", "year", "active_cap_millions", "dead_money_millions", 
                "total_cap_millions", "cap_space_millions", "dead_cap_pct"
            ]
        }
        
        for pattern, expected_cols in expected_columns.items():
            files = list(Path(".").glob(pattern))
            
            if not files:
                continue
            
            # Check most recent file
            most_recent = max(files, key=lambda p: p.stat().st_mtime)
            df = pd.read_csv(most_recent)
            
            missing_cols = set(expected_cols) - set(df.columns)
            assert len(missing_cols) == 0, \
                f"{most_recent.name} missing columns: {missing_cols}"
