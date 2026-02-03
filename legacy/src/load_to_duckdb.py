#!/usr/bin/env python3
"""
Load processed compensation data into DuckDB for dbt transformations.
Bypasses dbt seed complexity by directly ingesting CSVs into DuckDB staging tables.
"""

import duckdb
import pandas as pd
from pathlib import Path
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'data/processed/compensation'
DUCKDB_PATH = PROJECT_ROOT / 'dbt' / 'nfl_dead_money.duckdb'

def load_to_duckdb(year: int = 2024):
    """Load processed 2024 data into DuckDB for dbt marts."""
    
    conn = duckdb.connect(str(DUCKDB_PATH))
    
    try:
        # Create staging schema if not exists
        conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
        conn.execute("CREATE SCHEMA IF NOT EXISTS marts")
        
        # Load each processed file into DuckDB
        files = {
            'stg_player_contracts': f'stg_player_contracts_{year}.csv',
            'stg_dead_money': f'stg_dead_money_{year}.csv',
            'stg_player_rankings': f'stg_player_rankings_{year}.csv',
            'stg_team_cap': f'stg_team_cap_{year}.csv',
            'dead_money_features': f'dead_money_features_{year}.csv',
        }
        
        for table_name, filename in files.items():
            filepath = DATA_DIR / filename
            
            if not filepath.exists():
                logger.warning(f"  Skipping {table_name}: file not found ({filename})")
                continue
            
            # Read CSV
            df = pd.read_csv(filepath)
            
            # Determine schema
            schema = 'marts' if 'dead_money_features' in table_name else 'staging'
            full_name = f'{schema}.{table_name}'
            
            # Create table from CSV using DuckDB's read_csv
            conn.execute(f"DROP TABLE IF EXISTS {full_name}")
            conn.execute(f"CREATE TABLE {full_name} AS SELECT * FROM read_csv_auto('{filepath}')")
            
            row_count = conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchall()[0][0]
            logger.info(f"✓ Loaded {full_name}: {row_count} rows")
        
        # Create a simple mart view for dead money analysis
        conn.execute("""
            CREATE OR REPLACE VIEW marts.vw_dead_money_analysis AS
            SELECT 
                player_name,
                team,
                position,
                year,
                dead_cap_hit,
                guaranteed_money_millions,
                signing_bonus_millions,
                contract_length_years,
                age_at_signing,
                performance_av,
                games_played_prior_year,
                years_experience
            FROM marts.dead_money_features
            WHERE dead_cap_hit IS NOT NULL
            ORDER BY dead_cap_hit DESC
        """)
        logger.info("✓ Created mart view: vw_dead_money_analysis")
        
        # Show summary
        result = conn.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT team) as teams,
                SUM(dead_cap_hit) as total_dead_cap
            FROM marts.dead_money_features
            WHERE dead_cap_hit IS NOT NULL
        """).fetchdf()
        
        logger.info(f"\n📊 Mart Summary:\n{result.to_string()}")
        
        logger.info(f"\n✅ All data loaded to DuckDB: {DUCKDB_PATH}")
        
    finally:
        conn.close()

if __name__ == '__main__':
    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2024
    load_to_duckdb(year)
