"""
Data freshness tests for weekly pipeline runs.

Ensures scraped data is current and meets recency requirements.
"""

import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta


class TestDataFreshness:
    """Test that data is fresh enough for weekly runs."""

    def test_team_cap_data_exists_current_year(self):
        """Test that we have team cap data for current year."""
        current_year = datetime.now().year
        data_dir = Path("data/raw")
        
        team_cap_files = list(data_dir.glob(f"spotrac_team_cap_{current_year}_*.csv"))
        assert len(team_cap_files) > 0, \
            f"No team cap files found for {current_year} in {data_dir}"

    def test_team_cap_data_is_recent(self):
        """Test that most recent team cap file is <7 days old."""
        current_year = datetime.now().year
        data_dir = Path("data/raw")
        
        team_cap_files = list(data_dir.glob(f"spotrac_team_cap_{current_year}_*.csv"))
        if not team_cap_files:
            pytest.skip(f"No team cap files for {current_year}")
        
        # Get most recent file by mtime
        most_recent = max(team_cap_files, key=lambda p: p.stat().st_mtime)
        file_age_days = (datetime.now().timestamp() - most_recent.stat().st_mtime) / 86400
        
        assert file_age_days < 7, \
            f"Most recent team cap file is {file_age_days:.1f} days old (expected <7)"

    def test_processed_data_exists(self):
        """Test that processed data directory has expected files."""
        processed_dir = Path("data/processed/compensation")
        
        required_files = [
            "dim_players.csv",
            "fact_player_contracts.csv",
            "team_dead_money_by_year.csv"
        ]
        
        for filename in required_files:
            filepath = processed_dir / filename
            assert filepath.exists(), f"Missing required file: {filepath}"

    def test_no_future_year_data(self):
        """Test that we don't have data from future years (data quality check)."""
        current_year = datetime.now().year
        processed_dir = Path("data/processed/compensation")
        
        for csv_file in processed_dir.glob("*.csv"):
            df = pd.read_csv(csv_file)
            
            if 'year' in df.columns:
                max_year = df['year'].max()
                assert max_year <= current_year + 1, \
                    f"{csv_file.name} has data from year {max_year} (current: {current_year})"


class TestDataCompleteness:
    """Test that data is complete for expected date ranges."""

    def test_all_recent_years_present(self):
        """Test that we have data for past 3 years."""
        current_year = datetime.now().year
        expected_years = list(range(current_year - 2, current_year + 1))
        
        # Check team dead money file
        team_dm_file = Path("data/processed/compensation/team_dead_money_by_year.csv")
        if not team_dm_file.exists():
            pytest.skip("Team dead money file not found")
        
        df = pd.read_csv(team_dm_file)
        actual_years = set(df['year'].unique())
        
        missing_years = set(expected_years) - actual_years
        assert len(missing_years) == 0, \
            f"Missing data for years: {missing_years}"

    def test_all_teams_present_current_year(self):
        """Test that current year has all 32 teams."""
        current_year = datetime.now().year
        team_dm_file = Path("data/processed/compensation/team_dead_money_by_year.csv")
        
        if not team_dm_file.exists():
            pytest.skip("Team dead money file not found")
        
        df = pd.read_csv(team_dm_file)
        current_year_teams = df[df['year'] == current_year]['team'].nunique()
        
        assert current_year_teams >= 30, \
            f"Current year has only {current_year_teams} teams (expected ≥30)"


class TestWeeklyPipelineReadiness:
    """Test that pipeline is ready for weekly scheduled runs."""

    def test_parquet_sidecars_exist(self):
        """Test that Parquet sidecars are being generated."""
        parquet_dir = Path("data/processed/compensation/parquet")
        
        if not parquet_dir.exists():
            pytest.skip("Parquet directory not created yet")
        
        # Check for at least one table with partitions
        parquet_tables = list(parquet_dir.glob("*/year=*/part-000.parquet"))
        assert len(parquet_tables) > 0, \
            "No Parquet files found in sidecars"

    def test_duckdb_exists(self):
        """Test that DuckDB database exists."""
        duckdb_file = Path("nfl_dead_money.duckdb")
        assert duckdb_file.exists(), \
            "DuckDB database not found; run dbt first"

    def test_pipeline_scripts_exist(self):
        """Test that all required pipeline scripts are present."""
        required_scripts = [
            "scripts/scrape_player_salaries.py",
            "scripts/player_rankings_snapshot.py",
            "scripts/backfill_player_rankings.py",
            "src/roster_salary_merge.py",
            "src/normalization.py",
            "src/dead_money_validator.py"
        ]
        
        for script_path in required_scripts:
            filepath = Path(script_path)
            assert filepath.exists(), f"Missing required script: {filepath}"
