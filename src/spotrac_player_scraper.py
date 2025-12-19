"""
Robust Spotrac Scraper with Selenium and Data Quality Checks - PLAYER DEAD MONEY

Scrapes player-level dead money data from Spotrac using browser automation.
Includes comprehensive data quality validation at ingestion and transformation stages.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when data quality checks fail"""
    pass


def scrape_player_dead_money(year: int, headless: bool = True) -> pd.DataFrame:
    """
    Scrape player-level dead money data from Spotrac for a given year.
    
    Returns DataFrame with columns:
    - player_name: Player name
    - team: Team abbreviation
    - position: Player position
    - year: Season year
    - dead_money_millions: Dead money cap charge
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup
    
    url = f"https://www.spotrac.com/nfl/dead-money/{year}/"
    logger.info(f"Scraping player dead money: {url}")
    
    # Initialize driver
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    
    driver = webdriver.Chrome(options=options)
    
    try:
        driver.get(url)
        
        # Wait for table
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable"))
        )
        time.sleep(3)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Find table with tbody
        tables = soup.find_all('table', {'class': 'dataTable'})
        table = None
        for tbl in tables:
            if tbl.find('tbody'):
                table = tbl
                break
                
        if not table:
            raise DataQualityError("No player dead money table found")
            
        tbody = table.find('tbody')
        if not tbody:
            raise DataQualityError("Table missing tbody")
            
        # Extract headers
        thead = table.find('thead')
        headers = []
        if thead:
            headers = [th.text.strip().replace('\n', ' ') for th in thead.find_all('th')]
        
        logger.info(f"  Headers ({len(headers)}): {headers[:5]}")
        
        # Extract rows
        rows = []
        for tr in tbody.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) < 3:
                continue
            row = [td.text.strip().replace('\n', ' ') for td in tds]
            rows.append(row)
            
        # INGESTION QUALITY CHECK
        if len(rows) < 50:
            raise DataQualityError(f"Expected ≥50 player records, got {len(rows)}")
            
        logger.info(f"  ✓ Extracted {len(rows)} player records")
        
        # Build DataFrame
        df = pd.DataFrame(rows, columns=headers[:len(rows[0]) if rows else 0])
        
        # Normalize columns
        col_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if 'player' in col_lower or 'name' in col_lower:
                col_map[col] = 'player_name'
            elif 'team' in col_lower and 'avg' not in col_lower:
                col_map[col] = 'team'
            elif 'pos' in col_lower:
                col_map[col] = 'position'
            elif 'dead' in col_lower:
                col_map[col] = 'dead_money'
                
        df = df.rename(columns=col_map)
        
        # Parse money
        def parse_money(value: str) -> float:
            try:
                value = str(value).replace('$', '').replace(',', '').strip()
                if 'M' in value:
                    return float(value.replace('M', ''))
                elif 'K' in value:
                    return float(value.replace('K', '')) / 1000
                else:
                    return float(value) / 1_000_000
            except:
                return 0.0
        
        if 'dead_money' in df.columns:
            df['dead_money_millions'] = df['dead_money'].apply(parse_money)
        
        df['year'] = year
        
        # Keep relevant columns
        keep_cols = [c for c in ['player_name', 'team', 'position', 'year', 'dead_money_millions'] 
                    if c in df.columns]
        df = df[keep_cols]
        
        # DATA QUALITY CHECKS
        if len(df) < 50:
            raise DataQualityError(f"Expected ≥50 records, got {len(df)}")
            
        if df['player_name'].isnull().sum() > 0:
            raise DataQualityError("player_name has nulls")
            
        if 'dead_money_millions' in df.columns:
            total_dm = df['dead_money_millions'].sum()
            if total_dm < 500 or total_dm > 10000:
                raise DataQualityError(f"Total dead money ${total_dm:.1f}M seems unreasonable")
            
            unique_teams = df['team'].nunique()
            if unique_teams < 10:
                raise DataQualityError(f"Only {unique_teams} teams (expected ≥20)")
            
            logger.info(f"  ✓ Teams: {unique_teams}")
            logger.info(f"  ✓ Total dead money: ${total_dm:.1f}M")
            logger.info(f"  ✓ All quality checks passed")
        
        return df
        
    finally:
        driver.quit()


def scrape_and_save_player_dead_money(year: int, output_dir: str = 'data/raw') -> Path:
    """Scrape player dead money data and save to CSV with timestamp."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"spotrac_player_dead_money_{year}_{timestamp}.csv"
    filepath = output_path / filename
    
    df = scrape_player_dead_money(year)
    df.to_csv(filepath, index=False)
    
    logger.info(f"✓ Saved {len(df)} records to {filepath}")
    return filepath


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python src/spotrac_player_scraper.py YEAR [START_YEAR END_YEAR]")
        sys.exit(1)
    
    try:
        if len(sys.argv) == 2:
            # Single year
            year = int(sys.argv[1])
            filepath = scrape_and_save_player_dead_money(year)
            print(f"\n✅ SUCCESS: Data saved to {filepath}")
        else:
            # Range backfill
            start_year = int(sys.argv[1])
            end_year = int(sys.argv[2])
            
            print(f"\n🔄 Backfill player dead money for {start_year}-{end_year}\n")
            
            for year in range(start_year, end_year + 1):
                print(f"\n{'='*60}\nYear {year}\n{'='*60}")
                try:
                    filepath = scrape_and_save_player_dead_money(year)
                    print(f"✓ {filepath.name}")
                except Exception as e:
                    logger.error(f"Failed: {e}")
                    continue
            
            print(f"\n✅ Backfill complete")
            
    except DataQualityError as e:
        print(f"\n❌ DATA QUALITY FAILURE: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
