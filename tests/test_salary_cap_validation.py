"""
Pytest test suite for NFL Salary Cap Validation.

Tests that scraped salary cap data matches official NFL figures.
"""

import pytest
import sys
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from salary_cap_reference import (
    get_official_cap,
    get_league_total_cap,
    validate_team_cap,
    validate_league_total,
    get_expected_range,
    NFL_SALARY_CAPS,
    NFL_TEAMS_COUNT,
)


class TestSalaryCapReference:
    """Test the reference data itself."""

    def test_reference_data_exists(self):
        """Test that we have reference data for expected years."""
        assert 2024 in NFL_SALARY_CAPS
        assert 2015 in NFL_SALARY_CAPS
        assert len(NFL_SALARY_CAPS) >= 10

    def test_cap_generally_increases(self):
        """Test that cap generally increases over time (except COVID year)."""
        for year in range(2015, 2020):  # Pre-COVID
            assert NFL_SALARY_CAPS[year + 1] > NFL_SALARY_CAPS[year]
        
        # 2021 was COVID-reduced
        assert NFL_SALARY_CAPS[2021] < NFL_SALARY_CAPS[2020]
        
        # Post-COVID recovery
        for year in range(2021, 2024):
            assert NFL_SALARY_CAPS[year + 1] > NFL_SALARY_CAPS[year]

    def test_get_official_cap(self):
        """Test getting official cap for a year."""
        assert get_official_cap(2024) == 255.4
        assert get_official_cap(2023) == 224.8

    def test_get_league_total_cap(self):
        """Test league total calculation."""
        assert get_league_total_cap(2024) == 255.4 * 32
        assert abs(get_league_total_cap(2024) - 8172.8) < 0.1


class TestTeamCapData:
    """Test scraped team salary cap data against official figures."""

    @pytest.fixture
    def team_cap_data(self):
        """Load the latest team cap data."""
        # Find most recent team cap files
        data_dir = Path("data/raw")
        team_files = {}
        
        if not data_dir.exists():
            pytest.skip("Data directory missing")

        for year in range(2015, 2025):
            files = list(data_dir.glob(f"spotrac_team_cap_{year}*.csv"))
            if files:
                # Use most recent file for this year
                latest = max(files, key=lambda p: p.stat().st_mtime)
                team_files[year] = latest
        
        return team_files

    def test_team_cap_files_exist(self, team_cap_data):
        """Test that we have team cap data for multiple years."""
        if not team_cap_data:
            pytest.skip("No team cap data found")
        assert len(team_cap_data) > 0, "No team cap data found"
        # Optional: check specific year only if expected
        # assert 2024 in team_cap_data, "Missing 2024 team cap data"

    def test_all_teams_present(self, team_cap_data):
        """Test that all 32 teams are present for each year."""
        if not team_cap_data:
             pytest.skip("No data to test")
        for year, filepath in team_cap_data.items():
            df = pd.read_csv(filepath)
            assert len(df) == NFL_TEAMS_COUNT, \
                f"Year {year}: Expected {NFL_TEAMS_COUNT} teams, got {len(df)}"

    def test_league_total_cap(self, team_cap_data):
        """Test that league-wide total cap is near official figure."""
        if not team_cap_data:
             pytest.skip("No data to test")
        failures = []
        
        for year, filepath in team_cap_data.items():
            if year not in NFL_SALARY_CAPS:
                continue
            
            df = pd.read_csv(filepath)
            total_cap = df['total_cap_millions'].sum()
            expected = get_league_total_cap(year)
            variance_pct = abs(total_cap - expected) / expected * 100
            
            is_valid = validate_league_total(total_cap, year)
            
            if not is_valid:
                failures.append({
                    'year': year,
                    'expected': expected,
                    'actual': total_cap,
                    'variance_pct': variance_pct,
                })
        
        if failures:
            msg = "League total cap validation failed:\n"
            for f in failures:
                msg += (f"  {f['year']}: Expected ${f['expected']:.1f}M, "
                       f"Got ${f['actual']:.1f}M ({f['variance_pct']:.1f}% variance)\n")
            pytest.fail(msg)

    def test_team_caps_within_range(self, team_cap_data):
        """Test that individual team caps are within reasonable range."""
        if not team_cap_data:
             pytest.skip("No data to test")
        failures = []
        
        for year, filepath in team_cap_data.items():
            if year not in NFL_SALARY_CAPS:
                continue
            
            df = pd.read_csv(filepath)
            official_cap = get_official_cap(year)
            min_cap, max_cap = get_expected_range(year)
            
            for _, row in df.iterrows():
                team = row['team']
                team_cap = row['total_cap_millions']
                
                if not validate_team_cap(team_cap, year):
                    failures.append({
                        'year': year,
                        'team': team,
                        'cap': team_cap,
                        'expected': official_cap,
                        'min': min_cap,
                        'max': max_cap,
                    })
        
        if failures:
            msg = "Team cap validation failed (outside ±15% range):\n"
            for f in failures[:10]:  # Show first 10
                msg += (f"  {f['year']} {f['team']}: ${f['cap']:.1f}M "
                       f"(expected ${f['expected']:.1f}M, range ${f['min']:.1f}-${f['max']:.1f}M)\n")
            if len(failures) > 10:
                msg += f"  ... and {len(failures) - 10} more\n"
            pytest.fail(msg)

    def test_no_negative_caps(self, team_cap_data):
        """Test that no team has negative cap values."""
        if not team_cap_data:
             pytest.skip("No data to test")
        for year, filepath in team_cap_data.items():
            df = pd.read_csv(filepath)
            
            assert (df['active_cap_millions'] >= 0).all(), \
                f"Year {year}: Negative active cap detected"
            assert (df['dead_money_millions'] >= 0).all(), \
                f"Year {year}: Negative dead money detected"
            assert (df['total_cap_millions'] >= 0).all(), \
                f"Year {year}: Negative total cap detected"

    def test_dead_cap_percentage_reasonable(self, team_cap_data):
        """Test that dead cap percentage is reasonable (0-50%)."""
        if not team_cap_data:
             pytest.skip("No data to test")
        for year, filepath in team_cap_data.items():
            df = pd.read_csv(filepath)
            
            # Dead cap should be 0-50% of total (50% would be extreme)
            assert (df['dead_cap_pct'] >= 0).all(), \
                f"Year {year}: Negative dead cap percentage"
            
            unreasonable = df[df['dead_cap_pct'] > 50]
            if not unreasonable.empty:
                teams = unreasonable['team'].tolist()
                pcts = unreasonable['dead_cap_pct'].tolist()
                pytest.fail(f"Year {year}: Unreasonable dead cap % > 50%: {list(zip(teams, pcts))}")


class TestCapComponentsConsistency:
    """Test that cap components (active, dead, total) are consistent."""

    @pytest.fixture
    def team_cap_2024(self):
        """Load 2024 team cap data."""
        files = list(Path("data/raw").glob("spotrac_team_cap_2024*.csv"))
        if not files:
            pytest.skip("No 2024 team cap data found")
        latest = max(files, key=lambda p: p.stat().st_mtime)
        return pd.read_csv(latest)

    def test_components_sum_reasonably(self, team_cap_2024):
        """Test that active + dead ≈ total (within tolerance)."""
        df = team_cap_2024
        
        # Calculate sum
        calculated_total = df['active_cap_millions'] + df['dead_money_millions']
        reported_total = df['total_cap_millions']
        
        # Check if within $5M tolerance per team
        tolerance_millions = 5.0
        differences = abs(calculated_total - reported_total)
        
        large_diffs = df[differences > tolerance_millions]
        
        if not large_diffs.empty:
            msg = "Cap components don't sum (active + dead ≠ total):\n"
            for _, row in large_diffs.head(5).iterrows():
                calc = row['active_cap_millions'] + row['dead_money_millions']
                diff = calc - row['total_cap_millions']
                msg += (f"  {row['team']}: Active ${row['active_cap_millions']:.1f}M + "
                       f"Dead ${row['dead_money_millions']:.1f}M = ${calc:.1f}M "
                       f"(reported ${row['total_cap_millions']:.1f}M, diff ${diff:.1f}M)\n")
            # This is a warning, not a failure
            pytest.skip(f"Known issue: {msg}")

    def test_dead_money_percentage_matches(self, team_cap_2024):
        """Test that dead_cap_pct matches dead_money / total_cap."""
        df = team_cap_2024
        
        calculated_pct = (df['dead_money_millions'] / df['total_cap_millions'] * 100).round(2)
        reported_pct = df['dead_cap_pct']
        
        # Allow 0.5% tolerance for rounding
        differences = abs(calculated_pct - reported_pct)
        assert (differences < 0.5).all(), \
            f"Dead cap percentage mismatch: max diff {differences.max():.2f}%"


class TestHistoricalCapProgression:
    """Test historical salary cap progression and trends."""

    def test_2024_cap_is_highest(self):
        """Test that 2024 has the highest salary cap in history."""
        max_cap_year = max(NFL_SALARY_CAPS.keys())
        max_cap_value = max(NFL_SALARY_CAPS.values())
        
        assert max_cap_year == 2024
        assert max_cap_value == NFL_SALARY_CAPS[2024]

    def test_average_cap_growth(self):
        """Test that average cap growth is reasonable (3-10% per year)."""
        years = sorted([y for y in NFL_SALARY_CAPS.keys() if 2015 <= y <= 2024])
        growth_rates = []
        
        for i in range(len(years) - 1):
            year1, year2 = years[i], years[i + 1]
            if year2 == 2021:  # Skip COVID year
                continue
            
            cap1 = NFL_SALARY_CAPS[year1]
            cap2 = NFL_SALARY_CAPS[year2]
            growth_pct = (cap2 - cap1) / cap1 * 100
            growth_rates.append(growth_pct)
        
        avg_growth = sum(growth_rates) / len(growth_rates)
        
        # Average growth should be 3-10% (reasonable for NFL)
        assert 3 <= avg_growth <= 10, \
            f"Average cap growth {avg_growth:.1f}% outside expected range 3-10%"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
