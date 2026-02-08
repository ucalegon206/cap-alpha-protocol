import duckdb
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def sync():
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        logger.error("MOTHERDUCK_TOKEN environment variable not set.")
        sys.exit(1)

    local_db = os.getenv("DB_PATH", "data/nfl_data.db")
    if not os.path.exists(local_db):
        logger.error(f"Local database not found at {local_db}")
        sys.exit(1)

    # Sanity check: ensure local DB has data
    try:
        local_con = duckdb.connect(local_db)
        tables = local_con.execute("SHOW TABLES").fetchall()
        if not tables:
            logger.error("Local database is empty. Aborting sync to prevent data loss in MotherDuck.")
            local_con.close()
            sys.exit(1)
        # Check for important tables
        table_names = [t[0] for t in tables]
        if "fact_player_efficiency" not in table_names:
             logger.warning("Gold layer table 'fact_player_efficiency' not found in local DB.")
        local_con.close()
    except Exception as e:
        logger.error(f"Failed to verify local database: {e}")
        sys.exit(1)

    try:
        logger.info(f"Connecting to MotherDuck...")
        # Connecting with the token
        con = duckdb.connect(f"md:nfl_dead_money?motherduck_token={token}")
        
        logger.info(f"Pushing {local_db} to MotherDuck...")
        # Use CREATE OR REPLACE DATABASE to sync the entire state
        # In MotherDuck, this will overwrite the 'nfl_dead_money' database with the contents of the local file
        con.execute(f"CREATE OR REPLACE DATABASE nfl_dead_money FROM '{local_db}'")
        
        logger.info("✓ Sync to MotherDuck completed successfully.")
    except Exception as e:
        logger.error(f"Failed to sync to MotherDuck: {e}")
        sys.exit(1)

if __name__ == "__main__":
    sync()
