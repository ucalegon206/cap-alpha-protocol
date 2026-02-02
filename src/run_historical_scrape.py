import logging
import argparse
import sys
from pathlib import Path
import time
from spotrac_scraper_v2 import SpotracScraper
from pfr_game_logs import scrape_season_logs

# Configure logging to stdout for Airflow compatibility
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def scrape_spotrac_year(year: int, force: bool = False):
    """Scrape Spotrac financial data for a specific year."""
    logger.info(f"Starting Spotrac Scrape for {year}...")
    
    # Idempotency check
    existing = list(Path("data/raw").glob(f"spotrac_player_rankings_{year}_*.csv"))
    if existing and not force:
        logger.info(f"Skipping {year}: Data already exists at {existing[0]}")
        return

    try:
        with SpotracScraper(headless=True) as scraper:
            df = scraper.scrape_player_rankings(year)
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            outfile = f"data/raw/spotrac_player_rankings_{year}_{timestamp}.csv"
            df.to_csv(outfile, index=False)
            logger.info(f"Saved {len(df)} rankings to {outfile}")

            # Scrape Contracts (for Age and Details)
            try:
                msg = f"Starting Contracts Scrape for {year}..."
                logger.info(msg)
                df_con = scraper.scrape_player_contracts(year)
                outfile_con = f"data/raw/spotrac_player_contracts_{year}_{timestamp}.csv"
                df_con.to_csv(outfile_con, index=False)
                logger.info(f"Saved {len(df_con)} contracts to {outfile_con}")
            except Exception as e:
                logger.error(f"Contracts scrape warning: {e}")

            # Scrape Dead Money (for accurate cap hit/dead cap breakdown)
            try:
                msg = f"Starting Dead Money Scrape for {year}..."
                logger.info(msg)
                df_dead = scraper.scrape_player_salaries(year)
                outfile_dead = f"data/raw/spotrac_player_salaries_{year}_{timestamp}.csv"
                df_dead.to_csv(outfile_dead, index=False)
                logger.info(f"Saved {len(df_dead)} dead money records to {outfile_dead}")
            except Exception as e:
                logger.error(f"Dead Money scrape warning: {e}")
            
    except Exception as e:
        logger.error(f"Spotrac Scrape failed for {year}: {e}")
        sys.exit(1) # Fail the task for Airflow to retry

def scrape_pfr_year(year: int, force: bool = False):
    """Scrape PFR game logs for a specific year."""
    logger.info(f"Starting PFR Game Logs Scrape for {year}...")
    
    # Idempotency check
    outfile = Path(f"data/raw/pfr/game_logs_{year}.csv")
    if outfile.exists() and not force:
        logger.info(f"Skipping {year}: Data already exists at {outfile}")
        return

    try:
        # Determine season length
        last_week = 18 if year >= 2021 else 17
        scrape_season_logs(year, start_week=1, end_week=last_week)
        
    except Exception as e:
        logger.error(f"PFR Scrape failed for {year}: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="NFL Historical Data Scraper")
    parser.add_argument("--year", type=int, required=True, help="Season year to scrape")
    parser.add_argument("--source", choices=['spotrac', 'pfr', 'all'], default='all', help="Data source to scrape")
    parser.add_argument("--force", action='store_true', help="Force re-scrape even if data exists")
    
    args = parser.parse_args()
    
    logger.info(f"Initializing task: Year={args.year}, Source={args.source}, Force={args.force}")
    
    if args.source in ['spotrac', 'all']:
        scrape_spotrac_year(args.year, force=args.force)
        
    if args.source in ['pfr', 'all']:
        scrape_pfr_year(args.year, force=args.force)
        
    logger.info("Task completed successfully.")

if __name__ == "__main__":
    main()
