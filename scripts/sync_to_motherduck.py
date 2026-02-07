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
