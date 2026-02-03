import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
from pathlib import Path
import argparse
from typing import List, Optional
import time
from io import StringIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def scrape_nfl_penalties(year: int) -> pd.DataFrame:
    """
    Scrapes player penalties from nflpenalties.com.
    Updated to include the player link for better matching.
    """
    url = f"https://nflpenalties.com/all-players.php?year={year}"
    logger.info(f"Fetching penalties from {url}")
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.text, 'lxml')
        table = soup.find('table')
        if not table:
            return pd.DataFrame()
            
        rows = []
        # Extract headers from the thead if it exists, otherwise use standard names
        headers = [th.text.strip() for th in table.find_all('th')]
        if not headers:
            headers = ['Name', 'Pos', 'Team', 'Penalty Ct', 'Yds', 'Declined', 'Offsetting', 'Total Flags', 'Pre-Snap', 'Ct/Game', 'Yds/Game', '% of Team', 'Penalties']

        for tr in table.find_all('tr')[1:]: # Skip header row
            cols = tr.find_all('td')
            if not cols: continue
            
            row_data = [c.text.strip() for c in cols]
            
            # Extract player slug from the first column's link
            link = cols[0].find('a')
            slug = link['href'] if link else ""
            
            row_dict = dict(zip(headers, row_data))
            row_dict['player_slug'] = slug
            rows.append(row_dict)
            
        df = pd.DataFrame(rows)
        
        # Clean columns
        df = df.rename(columns={
            'Name': 'player_name_short',
            'Pos': 'position',
            'Team': 'team_city',
            'Penalty Ct': 'penalty_count',
            'Yds': 'penalty_yards'
        })
        
        df['year'] = year
        
        logger.info(f"Successfully scraped {len(df)} player penalty records for {year}")
        return df
        
    except Exception as e:
        logger.error(f"Error scraping {year}: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--output", type=str, default="data/raw/penalties")
    args = parser.parse_args()
    
    df = scrape_nfl_penalties(args.year)
    if not df.empty:
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = int(time.time())
        filename = f"improved_penalties_{args.year}_{timestamp}.csv"
        df.to_csv(output_dir / filename, index=False)
        logger.info(f"Saved to {output_dir / filename}")
