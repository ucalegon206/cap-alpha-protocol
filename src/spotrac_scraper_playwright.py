"""
Playwright-based Spotrac Player Rankings Scraper

Uses Playwright for better memory management and handling of heavy JavaScript pages.
Includes comprehensive data quality validation.
"""

import pandas as pd
import logging
from pathlib import Path
import time
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when data quality checks fail"""
    pass


def scrape_player_rankings_playwright(year: int) -> pd.DataFrame:
    """
    Scrape player-level salary/cap hit data from Spotrac rankings page using Playwright.
    Shows all NFL players ranked by salary cap hit for a given year.
    
    Returns DataFrame with columns:
    - player_name: Player name
    - team: Team abbreviation  
    - position: Player position
    - year: Season year
    - cap_hit_millions: Salary cap hit for this year
    """
    from bs4 import BeautifulSoup
    
    url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
    logger.info(f"Scraping player rankings (cap hit): {url}")
    
    with sync_playwright() as p:
        # Launch browser
        logger.info("  Starting Playwright browser...")
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        )
        page = context.new_page()
        
        try:
            # Navigate and wait for content
            logger.info("  Loading page...")
            page.goto(url, wait_until='load', timeout=120000)
            
            # Wait for network to settle
            logger.info("  Waiting for network to settle...")
            time.sleep(15)
            
            # Try to wait for DataTables to initialize
            logger.info("  Checking for DataTables initialization...")
            try:
                # Wait for any table element
                page.wait_for_selector('table', timeout=60000, state='attached')
                logger.info("  ✓ Table element found")
            except Exception as e:
                logger.warning(f"  ⚠️ Table wait failed: {e}")
                logger.info("  Proceeding anyway...")
            
            # Extra wait for DataTables JavaScript
            logger.info("  Waiting for DataTables to render...")
            time.sleep(20)
            
            # Check if table has rows
            try:
                row_count = page.locator('table tbody tr').count()
                logger.info(f"  Found {row_count} rows in table")
            except:
                logger.warning("  Could not count rows")
            
            # Get page content
            html = page.content()
            logger.info(f"  Page retrieved ({len(html)} bytes)")
            
        finally:
            browser.close()
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find any table - be flexible with table class names
    tables = soup.find_all('table')
    logger.info(f"  Found {len(tables)} tables")
    
    table = None
    
    # First try for dataTable class
    for tbl in tables:
        if 'dataTable' in tbl.get('class', []):
            if tbl.find('tbody'):
                table = tbl
                break
    
    # If not found, use first table with tbody
    if not table:
        for tbl in tables:
            if tbl.find('tbody'):
                table = tbl
                break
            
    if not table:
        raise DataQualityError("No player rankings table found")
        
    tbody = table.find('tbody')
    if not tbody:
        raise DataQualityError("Table missing tbody")
        
    # Extract headers
    thead = table.find('thead')
    headers = []
    if thead:
        headers = [th.text.strip().replace('\n', ' ') for th in thead.find_all('th')]
    
    logger.info(f"  Headers ({len(headers)}): {headers[:6]}")
    
    # Extract rows
    rows = []
    for tr in tbody.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) < 3:
            continue
        row = []
        for td in tds:
            text = td.text.strip().replace('\n', ' ')
            row.append(text)
        rows.append(row)
        
    # INGESTION QUALITY CHECK: Minimum row count
    if len(rows) < 500:
        raise DataQualityError(f"Expected ≥500 player records, got {len(rows)}")
        
    logger.info(f"  ✓ Extracted {len(rows)} player records")
    
    # Build DataFrame
    df = pd.DataFrame(rows, columns=headers[:len(rows[0]) if rows else 0])
    
    # TRANSFORMATION: Normalize columns
    df = _normalize_player_ranking_df(df, year)
    
    # TRANSFORMATION QUALITY CHECKS
    _validate_player_ranking_data(df, year)
    
    logger.info(f"  ✓ All quality checks passed")
    return df


def _normalize_player_ranking_df(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Normalize player ranking columns from rankings page"""
    
    # Rename columns - rankings page has different structure
    col_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if 'rank' in col_lower:
            col_map[col] = 'rank'
        elif 'player' in col_lower or 'name' in col_lower:
            col_map[col] = 'player_name'
        elif 'team' in col_lower and 'avg' not in col_lower:
            col_map[col] = 'team'
        elif 'pos' in col_lower:
            col_map[col] = 'position'
        elif 'salary' in col_lower or 'compensation' in col_lower:
            col_map[col] = 'salary'
        elif 'cap' in col_lower and 'hit' in col_lower:
            col_map[col] = 'cap_hit'
        elif 'dead' in col_lower:
            col_map[col] = 'dead_money'
            
    df = df.rename(columns=col_map)
    
    # Parse money columns
    money_cols = ['salary', 'cap_hit', 'dead_money']
    for col in money_cols:
        if col in df.columns:
            df[f'{col}_millions'] = df[col].apply(_parse_money)
            
    # Add year
    df['year'] = year
    
    # Keep only useful columns
    keep_cols = ['player_name', 'team', 'position', 'year', 
                 'salary_millions', 'cap_hit_millions', 'dead_money_millions']
    df = df[[c for c in keep_cols if c in df.columns]]
    
    return df


def _parse_money(value: str) -> float:
    """Parse money string like '$255.4M' or '$60,985,272' to millions"""
    try:
        value = str(value).replace('$', '').replace(',', '').strip()
        if 'M' in value:
            return float(value.replace('M', ''))
        elif 'K' in value:
            return float(value.replace('K', '')) / 1000
        elif 'B' in value:
            return float(value.replace('B', '')) * 1000
        else:
            # Raw dollar amount - convert to millions
            return float(value) / 1_000_000
    except:
        return 0.0


def _validate_player_ranking_data(df: pd.DataFrame, year: int):
    """Run data quality checks on player ranking data"""
    
    # Check 1: Minimum records (should have many players)
    if len(df) < 500:
        raise DataQualityError(f"Expected ≥500 players, got {len(df)}")
        
    # Check 2: Required columns
    required = ['player_name', 'team', 'year']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise DataQualityError(f"Missing columns: {missing}")
        
    # Check 3: No nulls in key columns
    if df['player_name'].isnull().sum() > 0:
        raise DataQualityError("player_name has nulls")
        
    # Check 4: Value ranges (if cap_hit data present)
    if 'cap_hit_millions' in df.columns:
        max_cap = df['cap_hit_millions'].max()
        min_cap = df['cap_hit_millions'].min()
        
        if max_cap > 100:  # Individual player cap hit shouldn't exceed ~$100M
            raise DataQualityError(f"Unreasonable max cap hit: ${max_cap}M")
            
        if min_cap < 0:
            raise DataQualityError("Negative cap hit values detected")
    
    # Check 5: Reasonable totals
    if 'cap_hit_millions' in df.columns:
        total_cap = df['cap_hit_millions'].sum()
        if total_cap < 1000 or total_cap > 50000:
            logger.warning(f"⚠️ Total cap hit ${total_cap:.1f}M seems unusual (expected ~6000-8000M)")
    
    # Check 6: Team coverage (should have multiple teams)
    unique_teams = df['team'].nunique()
    if unique_teams < 20:
        raise DataQualityError(f"Only {unique_teams} unique teams (expected ≥25)")
        
    logger.info(f"  ✓ Teams covered: {unique_teams}")
    logger.info(f"  ✓ Players recorded: {len(df)}")
    if 'cap_hit_millions' in df.columns:
        total_cap = df['cap_hit_millions'].sum()
        logger.info(f"  ✓ Total league cap hit: ${total_cap:.1f}M")


def scrape_and_save_player_rankings(year: int, output_dir: str = 'data/raw') -> Path:
    """
    Scrape player rankings (cap hit) data and save to CSV with timestamp.
    
    Returns path to saved file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"spotrac_player_rankings_{year}_{timestamp}.csv"
    filepath = output_path / filename
    
    df = scrape_player_rankings_playwright(year)
    df.to_csv(filepath, index=False)
        
    logger.info(f"✓ Saved {len(df)} records to {filepath}")
    
    return filepath


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python src/spotrac_scraper_playwright.py YEAR")
        print("  python src/spotrac_scraper_playwright.py backfill START_YEAR END_YEAR")
        sys.exit(1)
    
    command = sys.argv[1]
    
    try:
        if command == 'backfill':
            start_year = int(sys.argv[2])
            end_year = int(sys.argv[3])
            
            print(f"\n🔄 Starting player rankings backfill for {start_year}-{end_year}...\n")
            
            for year in range(start_year, end_year + 1):
                print(f"\n{'='*60}")
                print(f"Year {year}")
                print(f"{'='*60}")
                
                try:
                    player_path = scrape_and_save_player_rankings(year)
                    print(f"✓ Player rankings: {player_path.name}")
                except Exception as e:
                    logger.error(f"Failed to scrape player rankings for {year}: {e}")
                    continue
            
            print(f"\n✅ Backfill complete for {start_year}-{end_year}")
            
        else:
            # Single year
            year = int(command)
            filepath = scrape_and_save_player_rankings(year)
            print(f"\n✅ SUCCESS: Player rankings data saved to {filepath}")
            
    except DataQualityError as e:
        print(f"\n❌ DATA QUALITY FAILURE: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
