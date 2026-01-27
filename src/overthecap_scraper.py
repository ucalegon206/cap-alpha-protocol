"""
Over The Cap Contract Scraper

Over The Cap (www.overthecap.com) provides clean HTML contract tables without JavaScript.
More reliable than Spotrac for consistent scraping.

Scrapes:
- Team contracts page: /nfl/{team}/contracts/ 
- Extracts guaranteed money, signing bonus, contract years, cap hits
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, List
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed/compensation")
RAW_DIR = Path("data/raw")


def _build_timestamp() -> str:
    """Return ISO-week tag and timestamp for filenames."""
    now = datetime.utcnow()
    iso = now.isocalendar()
    iso_week_tag = f"{iso.year}w{iso.week:02d}"
    timestamp = now.strftime("%Y%m%d")
    return f"{iso_week_tag}_{timestamp}"


TEAM_MAP = {
    'ARI': 'Cardinals', 'ATL': 'Falcons', 'BAL': 'Ravens', 'BUF': 'Bills',
    'CAR': 'Panthers', 'CHI': 'Bears', 'CIN': 'Bengals', 'CLE': 'Browns',
    'DAL': 'Cowboys', 'DEN': 'Broncos', 'DET': 'Lions', 'GB': 'Packers',
    'HOU': 'Texans', 'IND': 'Colts', 'JAX': 'Jaguars', 'KC': 'Chiefs',
    'LAC': 'Chargers', 'LAR': 'Rams', 'LV': 'Raiders', 'MIA': 'Dolphins',
    'MIN': 'Vikings', 'NE': 'Patriots', 'NO': 'Saints', 'NYG': 'Giants',
    'NYJ': 'Jets', 'PHI': 'Eagles', 'PIT': 'Steelers', 'SF': '49ers',
    'SEA': 'Seahawks', 'TB': 'Buccaneers', 'TEN': 'Titans', 'WAS': 'Commanders'
}

# Reverse map for easier lookup
TEAM_TO_CODE = {v: k for k, v in TEAM_MAP.items()}


class OverTheCapScraper:
    """
    Over The Cap contract scraper.
    
    Advantages over Spotrac:
    - Clean HTML (no JavaScript rendering required)
    - Reliable table structure
    - No Selenium dependency (pure requests + BeautifulSoup)
    - Faster scraping
    """
    
    BASE_URL = "https://www.overthecap.com"
    
    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def scrape_team_contracts(self, year: int) -> pd.DataFrame:
        """
        Scrape all team contracts for a given year.
        
        Returns DataFrame with columns:
        - player_name: Player name
        - team: Team code (ARI, BAL, etc.)
        - position: Player position
        - year: Season year
        - total_value_millions: Total contract value
        - guaranteed_money_millions: Guaranteed money
        - signing_bonus_millions: Signing bonus
        - years_contracted: Contract length
        - year_cap_hit_millions: Current year cap hit
        """
        
        all_contracts = []
        team_codes = sorted(TEAM_MAP.keys())
        
        logger.info(f"Scraping Over The Cap contracts for {year} ({len(team_codes)} teams)")
        
        for team_code in team_codes:
            try:
                contracts = self._scrape_team(team_code, year)
                if contracts is not None and len(contracts) > 0:
                    all_contracts.append(contracts)
                    logger.info(f"  ✓ {team_code}: {len(contracts)} contracts")
                else:
                    logger.warning(f"  ⚠️ {team_code}: No contracts found")
            except Exception as e:
                logger.warning(f"  ✗ {team_code}: {e}")
                continue
            
            # Be respectful with requests
            time.sleep(0.5)
        
        if not all_contracts:
            logger.error("✗ No contracts scraped from any team")
            return pd.DataFrame()
        
        df = pd.concat(all_contracts, ignore_index=True)
        logger.info(f"✓ Scraped {len(df)} total player contracts")
        return df
    
    def _scrape_team(self, team_code: str, year: int) -> Optional[pd.DataFrame]:
        """Scrape contracts for a single team."""
        
        # Over The Cap uses lowercase team names in URLs
        team_name_lower = TEAM_MAP[team_code].lower()
        url = f"{self.BASE_URL}/nfl/{team_name_lower}/{year}/contracts/"
        
        logger.debug(f"    Fetching: {url}")
        
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find contracts table - Over The Cap uses different class names
        # Look for table containing contract data
        table = None
        for tbl in soup.find_all('table'):
            # Check if table has contract-like headers
            headers = [th.get_text().strip().lower() for th in tbl.find_all('th')]
            if any(h in ['player', 'name'] for h in headers) and any(h in ['guaranteed', 'bonus', 'cap'] for h in headers):
                table = tbl
                break
        
        if not table:
            logger.debug(f"    No contract table found for {team_code}")
            return None
        
        contracts = []
        tbody = table.find('tbody')
        if not tbody:
            tbody = table  # Some tables don't have tbody
        
        for row in tbody.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 5:
                continue
            
            try:
                contract = self._parse_contract_row(cells, team_code, year)
                if contract:
                    contracts.append(contract)
            except Exception as e:
                logger.debug(f"    Error parsing row: {e}")
                continue
        
        if contracts:
            return pd.DataFrame(contracts)
        return None
    
    def _parse_contract_row(self, cells: List, team_code: str, year: int) -> Optional[Dict]:
        """Parse a single contract table row."""
        
        def _parse_money(text: str) -> Optional[float]:
            """Parse money value (e.g., '$5,000,000' → 5.0)"""
            if not text:
                return None
            # Remove $, commas, whitespace
            clean = text.replace('$', '').replace(',', '').strip()
            try:
                value_dollars = float(clean)
                return value_dollars / 1_000_000  # Convert to millions
            except ValueError:
                return None
        
        # Over The Cap table structure varies, but typically:
        # [Player] [Position] [Contract Length] [Signing Bonus] [Guaranteed Money] [Cap Hit] [Dead Cap]
        
        player_name = cells[0].get_text().strip() if len(cells) > 0 else None
        position = cells[1].get_text().strip() if len(cells) > 1 else None
        
        if not player_name:
            return None
        
        # Try to find guaranteed and signing bonus columns by header context
        row_text = ' '.join([c.get_text().strip() for c in cells])
        
        # Extract monetary values from all cells
        money_values = []
        for cell in cells[2:]:  # Skip player and position
            val = _parse_money(cell.get_text())
            if val is not None:
                money_values.append(val)
        
        # Assign values (heuristic: typically guaranteed is largest, signing bonus next)
        guaranteed = money_values[0] if len(money_values) > 0 else None
        signing_bonus = money_values[1] if len(money_values) > 1 else None
        cap_hit = money_values[2] if len(money_values) > 2 else None
        
        # Try to extract contract length (years)
        years_text = cells[2].get_text().strip() if len(cells) > 2 else ""
        years_contracted = None
        if any(c.isdigit() for c in years_text):
            try:
                years_contracted = int(''.join(filter(str.isdigit, years_text.split()[0])))
            except:
                years_contracted = None
        
        return {
            'player_name': player_name,
            'team': team_code,
            'position': position,
            'year': year,
            'total_value_millions': guaranteed,  # Use guaranteed as proxy for total
            'guaranteed_money_millions': guaranteed,
            'signing_bonus_millions': signing_bonus,
            'years_contracted': years_contracted,
            'year_cap_hit_millions': cap_hit
        }
    
    def scrape_and_save(self, year: int) -> Path:
        """Scrape contracts and save to CSV."""
        df = self.scrape_team_contracts(year)
        
        if df.empty:
            logger.error("✗ No data scraped")
            raise Exception("Scraping failed: empty DataFrame")
        
        # Validate
        if len(df) < 25:
            logger.warning(f"⚠️ Only {len(df)} contracts scraped (expected 100+)")
        
        # Save
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = _build_timestamp()
        out_path = RAW_DIR / f"overthecap_player_contracts_{year}_{timestamp}.csv"
        df.to_csv(out_path, index=False)
        logger.info(f"✓ Saved: {out_path}")
        
        return out_path


def main():
    """CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape Over The Cap contract data')
    parser.add_argument('--year', type=int, required=True, help='Year to scrape')
    
    args = parser.parse_args()
    
    try:
        scraper = OverTheCapScraper()
        scraper.scrape_and_save(args.year)
        logger.info("✓ Scraping complete")
    except Exception as e:
        logger.error(f"✗ Scraping failed: {e}")
        raise


if __name__ == '__main__':
    main()
