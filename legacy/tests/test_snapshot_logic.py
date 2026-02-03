import pytest
from pathlib import Path
from src.spotrac_scraper_v2 import SpotracScraper

def test_save_snapshot(tmp_path):
    """Verify that snapshots are saved to the correct directory"""
    scraper = SpotracScraper(headless=True)
    scraper.snapshot_dir = tmp_path / "snapshots"
    
    test_html = "<html><body>Test</body></html>"
    scraper.save_snapshot(test_html, "test_snapshot")
    
    snapshot_file = scraper.snapshot_dir / "test_snapshot.html"
    assert snapshot_file.exists()
    assert snapshot_file.read_text() == test_html
