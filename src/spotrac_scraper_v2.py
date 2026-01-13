"""
Robust Spotrac Scraper with Selenium and Data Quality Checks

Scrapes team cap data and player salaries from Spotrac using browser automation.
Includes comprehensive data quality validation at ingestion and transformation stages.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, List
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _build_run_tags(run_timestamp: Optional[datetime] = None) -> tuple[str, str]:
    """Return iso-week tag and timestamp strings for consistent filenames."""
    ts = run_timestamp or datetime.utcnow()
    iso = ts.isocalendar()
    iso_week_tag = f"{iso.year}w{iso.week:02d}"
    timestamp = ts.strftime("%Y%m%d_%H%M%S")
    return iso_week_tag, timestamp


class DataQualityError(Exception):
    """Raised when data quality checks fail"""
    pass


class SpotracScraper:
    """
    Spotrac scraper with built-in data quality checks.
    
    Methods:
    - scrape_team_cap(): Team-level dead money and cap data
    - scrape_player_salaries(): Individual player salaries and dead money
    
    Quality gates:
    - Ingestion: Verify table found, row counts, column presence
    - Transformation: Check nulls, value ranges, data types
    - Completeness: Ensure reasonable data distributions and totals
    """
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        
    def __enter__(self):
        self._initialize_driver()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()
            
    def _initialize_driver(self):
        """Initialize Selenium Chrome driver"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
        except ImportError:
            raise ImportError("Selenium not installed. Run: pip install selenium")
            
        options = Options()
        if self.headless:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-web-resources")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-sync")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-default-apps")
        options.add_argument("--disable-preconnect")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-component-extensions-with-background-pages")
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        
        # Memory management
        options.add_argument("--memory-pressure-off")
        options.add_argument("--disable-renderer-backgrounding")
        
        self.driver = webdriver.Chrome(options=options)
        logger.info("✓ Selenium driver initialized")
        
    def scrape_team_cap(self, year: int) -> pd.DataFrame:
        """
        Scrape team cap data for a given year with quality checks.
        
        Returns DataFrame with columns:
        - team: Team name
        - year: Season year
        - active_cap_millions: Active player cap
        - dead_money_millions: Dead money
        - salary_cap_millions: Total salary cap
        - cap_space_millions: Available cap space
        - dead_cap_pct: Dead money as % of total cap
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from bs4 import BeautifulSoup
        
        url = f"https://www.spotrac.com/nfl/cap/{year}/"
        logger.info(f"Scraping team cap data: {url}")
        
        self.driver.get(url)
        
        # Wait for DataTable to load (Spotrac uses DataTables JS library)
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable"))
            )
            time.sleep(3)  # Let JS finish rendering
        except Exception as e:
            raise DataQualityError(f"Failed to load table: {e}")
            
        # Parse with BeautifulSoup
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        # Find the table with tbody (DataTables creates multiple table elements)
        tables = soup.find_all('table', {'class': 'dataTable'})
        table = None
        for tbl in tables:
            if tbl.find('tbody'):
                table = tbl
                break
                
        if not table:
            raise DataQualityError("No dataTable with tbody found in page")
            
        # INGESTION QUALITY CHECK 1: Table structure
        thead = table.find('thead')
        tbody = table.find('tbody')
        
        if not tbody:
            raise DataQualityError("Table missing tbody")
            
        # Extract headers
        headers = []
        if thead:
            headers = [th.text.strip().replace('\n', ' ') for th in thead.find_all('th')]
        else:
            # Fallback: infer from first row
            first_row = tbody.find('tr')
            if first_row:
                headers = [td.text.strip().replace('\n', ' ') for td in first_row.find_all(['th', 'td'])]
                
        logger.info(f"  Headers ({len(headers)}): {headers[:10]}")
        
        # Extract rows (skip totals row if present)
        rows = []
        for tr in tbody.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) < 5:
                continue
            # Extract text and clean team names
            row = []
            for td in tds:
                text = td.text.strip().replace('\n', ' ')
                # Clean team abbreviations (e.g., "SF  SF" -> "SF")
                if len(text) > 5 and text.count(' ') >= 2:
                    parts = text.split()
                    if len(parts[0]) <= 3 and parts[0] == parts[-1]:
                        text = parts[0]
                row.append(text)
            rows.append(row)
            
        # INGESTION QUALITY CHECK 2: Minimum row count
        if len(rows) < 30:
            raise DataQualityError(f"Expected ≥30 teams, got {len(rows)}")
            
        logger.info(f"  ✓ Extracted {len(rows)} team records")
        
        # Build DataFrame
        df = pd.DataFrame(rows, columns=headers[:len(rows[0])])
        
        # TRANSFORMATION: Normalize columns
        df = self._normalize_team_cap_df(df, year)
        
        # TRANSFORMATION QUALITY CHECKS
        self._validate_team_cap_data(df, year)
        
        logger.info(f"  ✓ All quality checks passed")
        return df
        
    def _normalize_team_cap_df(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """Normalize column names and parse monetary values"""
        
        # Rename columns to standard names (handle multiline headers)
        col_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if 'team' in col_lower and 'avg' not in col_lower:
                col_map[col] = 'team'
            elif 'active' in col_lower and '53' in col_lower:
                col_map[col] = 'active_cap'
            elif 'dead' in col_lower:
                col_map[col] = 'dead_money'
            elif 'total cap' in col_lower or 'allocations' in col_lower:
                col_map[col] = 'total_cap'
            elif 'space' in col_lower:
                col_map[col] = 'cap_space'
                
        df = df.rename(columns=col_map)
        
        # Parse money columns
        money_cols = ['active_cap', 'dead_money', 'total_cap', 'cap_space']
        for col in money_cols:
            if col in df.columns:
                df[f'{col}_millions'] = df[col].apply(self._parse_money)
                
        # Add year
        df['year'] = year
        
        # Calculate dead cap percentage
        if 'dead_money_millions' in df.columns and 'total_cap_millions' in df.columns:
            df['dead_cap_pct'] = (df['dead_money_millions'] / df['total_cap_millions'] * 100).round(2)
            
        # Keep only normalized columns
        keep_cols = ['team', 'year', 'active_cap_millions', 'dead_money_millions', 
                     'total_cap_millions', 'cap_space_millions', 'dead_cap_pct']
        df = df[[c for c in keep_cols if c in df.columns]]
        
        return df
        
    def _parse_money(self, value: str) -> float:
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
            
    def _validate_team_cap_data(self, df: pd.DataFrame, year: int):
        """Run data quality checks on team cap data"""
        
        # Check 1: Row count (should be 32 teams)
        if len(df) < 30 or len(df) > 35:
            raise DataQualityError(f"Expected 32 teams, got {len(df)}")
            
        # Check 2: Required columns present
        required = ['team', 'year', 'dead_money_millions', 'total_cap_millions']
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise DataQualityError(f"Missing required columns: {missing}")
            
        # Check 3: No nulls in critical columns
        for col in required:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                raise DataQualityError(f"Column '{col}' has {null_count} nulls")
                
        # Check 4: Value ranges
        if df['dead_money_millions'].max() > 1000:
            raise DataQualityError(f"Dead money values seem too high (max={df['dead_money_millions'].max()})")
            
        if df['total_cap_millions'].min() < 100:
            raise DataQualityError(f"Total cap values seem too low (min={df['total_cap_millions'].min()})")
            
        # Check 5: Reasonable totals
        total_dead_money = df['dead_money_millions'].sum()
        if total_dead_money < 100 or total_dead_money > 10000:
            raise DataQualityError(f"Total league dead money {total_dead_money}M seems unreasonable")
            
        logger.info(f"  ✓ Total league dead money: ${total_dead_money:.1f}M")
        logger.info(f"  ✓ Avg per team: ${total_dead_money/len(df):.1f}M")


    def scrape_player_rankings(self, year: int) -> pd.DataFrame:
        """
        Scrape player-level salary/cap hit data from Spotrac rankings page.
        Shows all NFL players ranked by salary cap hit for a given year.
        
        Returns DataFrame with columns:
        - player_name: Player name
        - team: Team abbreviation
        - position: Player position
        - year: Season year
        - cap_hit_millions: Salary cap hit for this year
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from bs4 import BeautifulSoup
        
        # Use full ranks page for cap hit - shows all players ranked by cap hit
        url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
        logger.info(f"Scraping player rankings (cap hit): {url}")
        
        self.driver.get(url)
        
        # Wait for table with very aggressive timeouts and JavaScript execution
        try:
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            
            # Try to wait for table with extended timeout
            logger.info("  Waiting for table (up to 60 seconds)...")
            WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
            )
            logger.info("  ✓ Table found, waiting for full render...")
            time.sleep(10)  # Extended wait for all rows to render
            
        except Exception as e:
            logger.warning(f"⚠️ Timeout waiting for table: {e}")
            logger.info("  Attempting JavaScript injection to trigger table rendering...")
            
            # Try to trigger DataTables initialization via JavaScript
            try:
                self.driver.execute_script("""
                    // Force DataTables to initialize if not already done
                    if (typeof $ !== 'undefined' && $.fn.DataTable) {
                        var tables = document.querySelectorAll('table');
                        tables.forEach(t => {
                            if (!$.fn.DataTable.isDataTable(t)) {
                                console.log('Initializing DataTable...');
                                // This might trigger table rendering
                            }
                        });
                    }
                    // Also scroll to trigger lazy loading
                    window.scrollBy(0, document.body.scrollHeight);
                """)
                logger.info("  Executed JavaScript, waiting 10 more seconds...")
                time.sleep(10)
            except Exception as js_err:
                logger.warning(f"  JavaScript execution failed: {js_err}")
        
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        # Find any table - be flexible with table class names
        tables = soup.find_all('table')
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
        df = self._normalize_player_ranking_df(df, year)
        
        # TRANSFORMATION QUALITY CHECKS
        self._validate_player_ranking_data(df, year)
        
        logger.info(f"  ✓ All quality checks passed")
        return df

    def scrape_player_salaries(self, year: int) -> pd.DataFrame:
        """
        Scrape player-level dead money data from Spotrac for a given year.
        
        Returns DataFrame with columns:
        - player_name: Player name
        - team: Team abbreviation
        - position: Player position
        - year: Season year
        - dead_money_millions: Dead money cap charge for this year
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from bs4 import BeautifulSoup
        
        # Use dead money page (most reliable for player-level data)
        url = f"https://www.spotrac.com/nfl/dead-money/{year}/"
        logger.info(f"Scraping player dead money: {url}")
        
        self.driver.get(url)
        
        # Wait for table
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable"))
            )
            time.sleep(3)
        except Exception as e:
            raise DataQualityError(f"Failed to load dead money table: {e}")
            
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
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
        if len(rows) < 50:
            raise DataQualityError(f"Expected ≥50 player records, got {len(rows)}")
            
        logger.info(f"  ✓ Extracted {len(rows)} player records")
        
        # Build DataFrame
        df = pd.DataFrame(rows, columns=headers[:len(rows[0]) if rows else 0])
        
        # TRANSFORMATION: Normalize columns
        df = self._normalize_player_dead_money_df(df, year)
        
        # TRANSFORMATION QUALITY CHECKS
        self._validate_player_dead_money_data(df, year)
        
        logger.info(f"  ✓ All quality checks passed")
        return df
        
    def _normalize_player_salary_df(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """Normalize player salary columns"""
        
        # Rename columns
        col_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if 'player' in col_lower or 'name' in col_lower:
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
                df[f'{col}_millions'] = df[col].apply(self._parse_money)
                
        # Add year
        df['year'] = year
        
        # Keep only useful columns
        keep_cols = ['player_name', 'team', 'position', 'year', 
                     'salary_millions', 'cap_hit_millions', 'dead_money_millions']
        df = df[[c for c in keep_cols if c in df.columns]]
        
        return df

    def _normalize_player_ranking_df(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
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
                df[f'{col}_millions'] = df[col].apply(self._parse_money)
                
        # Add year
        df['year'] = year
        
        # Keep only useful columns
        keep_cols = ['player_name', 'team', 'position', 'year', 
                     'salary_millions', 'cap_hit_millions', 'dead_money_millions']
        df = df[[c for c in keep_cols if c in df.columns]]
        
        return df
        
    def _validate_player_salary_data(self, df: pd.DataFrame, year: int):
        """Run data quality checks on player salary data"""
        
        # Check 1: Minimum records
        if len(df) < 100:
            raise DataQualityError(f"Expected ≥100 players, got {len(df)}")
            
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
        if unique_teams < 15:
            raise DataQualityError(f"Only {unique_teams} unique teams (expected ≥20)")
            
        logger.info(f"  ✓ Teams covered: {unique_teams}")
        if 'cap_hit_millions' in df.columns:
            total_cap = df['cap_hit_millions'].sum()
            logger.info(f"  ✓ Total league cap hit: ${total_cap:.1f}M")

    def _validate_player_ranking_data(self, df: pd.DataFrame, year: int):
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


def scrape_and_save_team_cap(
    year: int,
    output_dir: str = 'data/raw',
    run_timestamp: Optional[datetime] = None,
    iso_week_tag: Optional[str] = None,
) -> Path:
    """
    Scrape team cap data and save to CSV with timestamp.
    
    Returns path to saved file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    default_iso, default_ts = _build_run_tags(run_timestamp)
    if iso_week_tag:
        default_iso = iso_week_tag
    filename = f"spotrac_team_cap_{year}_{default_iso}_{default_ts}.csv"
    filepath = output_path / filename
    
    with SpotracScraper(headless=True) as scraper:
        df = scraper.scrape_team_cap(year)
        df.to_csv(filepath, index=False)
        
    logger.info(f"✓ Saved {len(df)} records to {filepath}")
    
    return filepath


def scrape_and_save_player_salaries(
    year: int,
    output_dir: str = 'data/raw',
    run_timestamp: Optional[datetime] = None,
    iso_week_tag: Optional[str] = None,
) -> Path:
    """
    Scrape player salary data and save to CSV with timestamp.
    
    Returns path to saved file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    default_iso, default_ts = _build_run_tags(run_timestamp)
    if iso_week_tag:
        default_iso = iso_week_tag
    filename = f"spotrac_player_salaries_{year}_{default_iso}_{default_ts}.csv"
    filepath = output_path / filename
    
    with SpotracScraper(headless=True) as scraper:
        df = scraper.scrape_player_salaries(year)
        df.to_csv(filepath, index=False)
        
    logger.info(f"✓ Saved {len(df)} records to {filepath}")
    
    return filepath


def scrape_and_save_player_rankings(
    year: int,
    output_dir: str = 'data/raw',
    run_timestamp: Optional[datetime] = None,
    iso_week_tag: Optional[str] = None,
) -> Path:
    """
    Scrape player rankings (cap hit) data and save to CSV with timestamp.
    
    Returns path to saved file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    default_iso, default_ts = _build_run_tags(run_timestamp)
    if iso_week_tag:
        default_iso = iso_week_tag
    filename = f"spotrac_player_rankings_{year}_{default_iso}_{default_ts}.csv"
    filepath = output_path / filename
    
    with SpotracScraper(headless=True) as scraper:
        df = scraper.scrape_player_rankings(year)
        df.to_csv(filepath, index=False)
        
    logger.info(f"✓ Saved {len(df)} records to {filepath}")
    
    return filepath


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python src/spotrac_scraper_v2.py team-cap YEAR")
        print("  python src/spotrac_scraper_v2.py player-salaries YEAR")
        print("  python src/spotrac_scraper_v2.py player-rankings YEAR")
        print("  python src/spotrac_scraper_v2.py backfill START_YEAR END_YEAR")
        sys.exit(1)
    
    command = sys.argv[1]
    run_timestamp = datetime.utcnow()
    
    try:
        if command == 'team-cap':
            year = int(sys.argv[2])
            filepath = scrape_and_save_team_cap(year, run_timestamp=run_timestamp)
            print(f"\n✅ SUCCESS: Team cap data saved to {filepath}")
            
        elif command == 'player-salaries':
            year = int(sys.argv[2])
            filepath = scrape_and_save_player_salaries(year, run_timestamp=run_timestamp)
            print(f"\n✅ SUCCESS: Player salary data saved to {filepath}")
            
        elif command == 'player-rankings':
            year = int(sys.argv[2])
            filepath = scrape_and_save_player_rankings(year, run_timestamp=run_timestamp)
            print(f"\n✅ SUCCESS: Player rankings data saved to {filepath}")
            
        elif command == 'backfill':
            start_year = int(sys.argv[2])
            end_year = int(sys.argv[3])
            
            print(f"\n🔄 Starting backfill for {start_year}-{end_year}...\n")
            
            success_count = 0
            fail_count = 0
            
            for year in range(start_year, end_year + 1):
                print(f"\n{'='*60}")
                print(f"Year {year}")
                print(f"{'='*60}")
                
                try:
                    # Team cap
                    team_path = scrape_and_save_team_cap(year, run_timestamp=run_timestamp)
                    print(f"✓ Team cap: {team_path.name}")
                    success_count += 1
                except Exception as e:
                    print(f"✗ Failed: {e}")
                    logger.error(f"Failed to scrape team cap for {year}: {e}")
                    fail_count += 1
                    continue
            
            print(f"\n{'='*60}")
            print(f"✅ Backfill complete!")
            print(f"   Success: {success_count}/{end_year - start_year + 1}")
            print(f"   Failed: {fail_count}/{end_year - start_year + 1}")
            print(f"{'='*60}")
            
        else:
            print(f"Unknown command: {command}")
            sys.exit(1)
            
    except DataQualityError as e:
        print(f"\n❌ DATA QUALITY FAILURE: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
