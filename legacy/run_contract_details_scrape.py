import argparse
from src.spotrac_scraper_v2 import SpotracScraper
from src.config import DATA_RAW_DIR
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scrape_year(year: int):
    logger.info(f"Starting Full League Contract Details Scrape for {year}...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    with SpotracScraper(headless=True) as scraper:
        # 1. Contracts (Guarantees, Total, etc.)
        df_contracts = scraper.scrape_player_contracts(year=year, max_retries=2)
        if not df_contracts.empty:
            filename_contracts = f"spotrac_player_contracts_{year}_{timestamp}.csv"
            out_path_contracts = DATA_RAW_DIR / filename_contracts
            df_contracts.to_csv(out_path_contracts, index=False)
            logger.info(f"Successfully saved {len(df_contracts)} contract records to {out_path_contracts}")
        else:
            logger.error(f"Contracts scrape for {year} returned empty DataFrame!")

        # 2. Salaries / Dead Money (Dead Cap)
        logger.info(f"Starting Dead Money Scrape for {year}...")
        df_dead = scraper.scrape_player_salaries(year=year)
        if not df_dead.empty:
            filename_dead = f"spotrac_player_salaries_{year}_{timestamp}.csv"
            out_path_dead = DATA_RAW_DIR / filename_dead
            df_dead.to_csv(out_path_dead, index=False)
            logger.info(f"Successfully saved {len(df_dead)} dead money records to {out_path_dead}")
        else:
            logger.error(f"Dead money scrape for {year} returned empty DataFrame!")

def main():
    parser = argparse.ArgumentParser(description="Scrape Spotrac Contract Details")
    parser.add_argument("--year", type=int, help="Specific year to scrape (e.g., 2024)")
    parser.add_argument("--start", type=int, help="Start year for range")
    parser.add_argument("--end", type=int, help="End year for range")
    
    args = parser.parse_args()
    
    if args.year:
        scrape_year(args.year)
    elif args.start and args.end:
        for y in range(args.start, args.end + 1):
            scrape_year(y)
    else:
        # Default to 2024 if no args
        scrape_year(2024)

if __name__ == "__main__":
    main()
