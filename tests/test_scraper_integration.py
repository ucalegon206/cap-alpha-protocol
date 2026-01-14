"""
Integration tests for scraper functions.

Tests that scrapers work end-to-end with live/mock data and handle errors gracefully.
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from spotrac_scraper_v2 import SpotracScraper, scrape_and_save_team_cap
from pfr_scraper import scrape_pfr_player_rosters, fetch_pfr_tables


class TestSpotracScraperIntegration:
    """Integration tests for Spotrac scraper."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_scrape_team_cap_current_year(self):
        """Test scraping current year team cap data."""
        current_year = datetime.now().year
        
        with SpotracScraper(headless=True) as scraper:
            df = scraper.scrape_team_cap(current_year)
            
            # Basic assertions
            assert not df.empty, "DataFrame should not be empty"
            assert len(df) >= 30, f"Expected ≥30 teams, got {len(df)}"
            assert 'team' in df.columns
            assert 'year' in df.columns
            assert 'dead_money_millions' in df.columns
            
            # Data quality
            assert df['dead_money_millions'].notna().all()
            assert (df['dead_money_millions'] >= 0).all()
            assert df['year'].iloc[0] == current_year

    @pytest.mark.slow
    @pytest.mark.integration
    def test_scrape_and_save_creates_file(self, tmp_path):
        """Test that scrape_and_save creates a timestamped file."""
        year = 2024
        output_dir = str(tmp_path)
        
        filepath = scrape_and_save_team_cap(year=year, output_dir=output_dir)
        
        assert filepath.exists(), f"Output file not created: {filepath}"
        assert str(year) in filepath.name
        assert filepath.suffix == '.csv'
        
        # Check file has data
        import pandas as pd
        df = pd.read_csv(filepath)
        assert len(df) >= 30

    def test_scraper_handles_invalid_year(self):
        """Test that scraper handles invalid years gracefully."""
        with pytest.raises(Exception):
            with SpotracScraper(headless=True) as scraper:
                scraper.scrape_team_cap(1950)  # Too old


class TestPFRScraperIntegration:
    """Integration tests for PFR scraper."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_pfr_tables_returns_dict(self):
        """Test that fetch_pfr_tables returns a dict of DataFrames."""
        url = "https://www.pro-football-reference.com/years/2024/index.htm"
        tables = fetch_pfr_tables(url, rate_limit=1.0)
        
        assert isinstance(tables, dict)
        assert len(tables) > 0
        
        # Check at least one table is a DataFrame
        for table_id, df in tables.items():
            assert hasattr(df, 'columns')  # DataFrame-like
            break

    @pytest.mark.slow
    @pytest.mark.integration  
    def test_scrape_rosters_recent_year(self, tmp_path):
        """Test scraping rosters for a recent year."""
        year = 2023
        save_path = tmp_path / f"rosters_{year}.csv"
        
        df = scrape_pfr_player_rosters(year=year, save_path=str(save_path))
        
        assert not df.empty
        assert 'team' in df.columns
        assert 'year' in df.columns
        assert df['year'].iloc[0] == year
        assert save_path.exists()


class TestScraperErrorHandling:
    """Test error handling and resilience."""

    def test_scraper_connection_timeout(self):
        """Test that scraper handles connection timeouts."""
        # Mock bad URL or timeout scenario
        with pytest.raises(Exception):
            with SpotracScraper(headless=True) as scraper:
                scraper.driver.set_page_load_timeout(0.001)
                scraper.scrape_team_cap(2024)

    def test_scraper_missing_table(self):
        """Test handling of pages without expected tables."""
        # This would require mocking the webdriver response
        pass  # TODO: Add with mock


class TestScraperDataQuality:
    """Test data quality gates in scrapers."""

    @pytest.mark.integration
    def test_team_cap_quality_gates(self):
        """Test that quality gates catch bad data."""
        from spotrac_scraper_v2 import DataQualityError
        import pandas as pd
        
        scraper = SpotracScraper(headless=True)
        
        # Test with intentionally bad data
        bad_df = pd.DataFrame({
            'team': ['SF'],
            'year': [2024],
            'dead_money_millions': [9999],  # Too high
            'total_cap_millions': [50]  # Too low
        })
        
        with pytest.raises(DataQualityError):
            scraper._validate_team_cap_data(bad_df, 2024)


@pytest.fixture
def mark_slow(request):
    """Skip slow tests unless explicitly requested."""
    if request.config.getoption("-m") != "slow":
        pytest.skip("Slow test; use -m slow to run")
