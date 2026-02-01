from src.spotrac_scraper_v2 import SpotracScraper
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

with SpotracScraper(headless=True) as scraper:
    # Fetch 2024 rankings
    data = scraper.scrape_player_rankings(2024)
    df = pd.DataFrame(data)
    print(df.head())
    print(f"Columns: {df.columns}")
    if 'age' in df.columns:
        print(f"Age Sample: {df['age'].unique()[:10]}")
