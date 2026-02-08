import os
import sys
import logging
import duckdb
import json

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

import argparse

def sync():
    # Parse CLI Arguments
    parser = argparse.ArgumentParser(description="Sync local DuckDB to MotherDuck and/or Frontend JSON.")
    parser.add_argument("--year", type=int, help="Specific year to dump for frontend (default: latest in DB).")
    parser.add_argument("--skip-motherduck", action="store_true", help="Skip syncing to MotherDuck cloud.")
    parser.add_argument("--skip-json", action="store_true", help="Skip dumping JSON for frontend.")
    args = parser.parse_args()

    # 1. Config & Validation
    token = os.getenv("MOTHERDUCK_TOKEN")
    
    local_db = os.getenv("DB_PATH", "data/duckdb/nfl_production.db")
    if not os.path.exists(local_db):
        logger.error(f"Local database not found at {local_db}")
        sys.exit(1)

    # 2. JSON Bridge (Frontend Hydration)
    if not args.skip_json:
        try:
            logger.info("Starting JSON Bridge Sync (Frontend Hydration)...")
            
            dump_con = duckdb.connect(local_db, read_only=True)
            
            # Verify tables exist
            tables = dump_con.execute("SELECT table_name FROM information_schema.tables").fetchall()
            table_names = [t[0] for t in tables]
            
            if "fact_player_efficiency" not in table_names:
                 logger.warning("Gold layer table 'fact_player_efficiency' not found in local DB. Skipping JSON dump.")
            else:
                target_year = args.year
                if not target_year:
                    # Determine latest year dynamically
                    res_year = dump_con.execute("SELECT MAX(year) FROM fact_player_efficiency").fetchone()
                    target_year = res_year[0] if res_year and res_year[0] else 2025
                    logger.info(f"Detected latest season: {target_year}")
                else:
                    logger.info(f"Using specified season: {target_year}")

                # Query for the roster data
                dump_query = f"SELECT * FROM fact_player_efficiency WHERE year = {target_year}"
                logger.info(f"Executing dump query: {dump_query}")
                
                # Execute and fetch without Pandas
                import decimal
                res = dump_con.execute(dump_query)
                columns = [desc[0] for desc in res.description]
                rows = res.fetchall()
                
                cleaned_records = []
                for row in rows:
                    record = {}
                    for col_name, val in zip(columns, row):
                        if isinstance(val, decimal.Decimal):
                            val = float(val)
                        record[col_name] = val
                    cleaned_records.append(record)
                    
                # Define output path
                output_path = os.path.join("web", "data", "roster_dump.json")
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                with open(output_path, 'w') as f:
                    json.dump(cleaned_records, f)
                    
                logger.info(f"✓ Dumped {len(cleaned_records)} records to {output_path}")
                
            dump_con.close()
            
        except Exception as e:
            logger.error(f"Failed during JSON dump: {e}")

    # 3. Motherduck Sync
    if not args.skip_motherduck:
        if token:
            try:
                logger.info(f"Connecting to MotherDuck...")
                con = duckdb.connect(f"md:nfl_dead_money?motherduck_token={token}")
                
                logger.info(f"Pushing {local_db} to MotherDuck...")
                con.execute(f"CREATE OR REPLACE DATABASE nfl_dead_money FROM '{local_db}'")
                
                logger.info("✓ Sync to MotherDuck completed successfully.")
            except Exception as e:
                logger.error(f"Failed to sync to MotherDuck: {e}")
                sys.exit(1)
        else:
            logger.warning("MOTHERDUCK_TOKEN not set. Skipping cloud sync.")
    else:
        logger.info("Skipping MotherDuck sync (--skip-motherduck provided).")

if __name__ == "__main__":
    sync()
