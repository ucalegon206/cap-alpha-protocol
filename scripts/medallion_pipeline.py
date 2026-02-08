import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
from typing import List, Optional

# Ensure project root is in path
sys.path.append(str(Path(__file__).parent.parent))
from src.db_manager import DBManager
from src.config_loader import get_db_path, get_bronze_dir
from src.financial_ingestion import load_team_financials, load_player_merch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_DIR = get_bronze_dir()

def clean_doubled_name(name):
    if not isinstance(name, str): return name
    parts = name.strip().split()
    if len(parts) >= 3 and parts[0] == parts[-1]:
        return " ".join(parts[1:])
    mid_idx = len(name) // 2
    if len(name) % 2 == 0:
        if name[:mid_idx] == name[mid_idx:]:
            return name[:mid_idx]
    if len(parts) >= 2:
        mid = len(parts) // 2
        if len(parts) % 2 == 0:
            if parts[:mid] == parts[mid:]:
                return " ".join(parts[:mid])
    return name

class BronzeLayer:
    """Bronze Layer: Raw Data Discovery & Reading."""
    @staticmethod
    def find_files(pattern: str, year: int) -> List[Path]:
        source_dirs = ['spotrac', 'pfr', 'penalties', 'dead_money']
        for source in source_dirs:
            year_dir = BRONZE_DIR / source / str(year)
            if year_dir.exists():
                files = list(year_dir.glob(f"{pattern}*.csv"))
                if files:
                    files.sort()
                    return [files[-1]]
        return []

class SilverLayer:
    """SilverLayer: Cleaning, Normalizing, and Loading into Structured Tables."""
    def __init__(self, db: DBManager):
        self.db = db

    def provision_schemas(self):
        logger.info("Provisioning Silver Layer schemas...")
        schemas = [
            "CREATE TABLE IF NOT EXISTS silver_pfr_game_logs (player_name VARCHAR, team VARCHAR, year INTEGER, game_url VARCHAR, Passing_Yds VARCHAR, Rushing_Yds VARCHAR, Receiving_Yds VARCHAR, Passing_TD VARCHAR, Rushing_TD VARCHAR, Receiving_TD VARCHAR)",
            "CREATE TABLE IF NOT EXISTS silver_penalties (player_name_short VARCHAR, team VARCHAR, year INTEGER, penalty_count INTEGER, penalty_yards INTEGER)",
            "CREATE TABLE IF NOT EXISTS silver_spotrac_contracts (player_name VARCHAR, team VARCHAR, year INTEGER, position VARCHAR, cap_hit_millions FLOAT, dead_cap_millions FLOAT, signing_bonus_millions FLOAT, age INTEGER)",
            "CREATE TABLE IF NOT EXISTS silver_spotrac_rankings (player_name VARCHAR, year INTEGER, ranking_cap_hit_millions FLOAT)",
            "CREATE TABLE IF NOT EXISTS silver_team_cap (team VARCHAR, year INTEGER, win_pct FLOAT)",
            "CREATE TABLE IF NOT EXISTS silver_player_metadata (full_name VARCHAR, birth_date VARCHAR, college VARCHAR, draft_round INTEGER, draft_pick INTEGER, experience_years INTEGER)",
            "CREATE TABLE IF NOT EXISTS silver_player_merch (Player VARCHAR, Rank INTEGER)",
            "CREATE TABLE IF NOT EXISTS silver_team_finance (Team VARCHAR, Year INTEGER, Revenue_M FLOAT, OperatingIncome_M FLOAT)",
            "CREATE TABLE IF NOT EXISTS silver_spotrac_salaries (player_name VARCHAR, team VARCHAR, year INTEGER, \"dead cap\" VARCHAR)",
            "CREATE TABLE IF NOT EXISTS silver_pfr_draft_history (player_name VARCHAR, team VARCHAR, year INTEGER, draft_round INTEGER, draft_pick INTEGER)"
        ]
        for sql in schemas:
            self.db.execute(sql)

    def ingest_spotrac(self, year: int):
        logger.info(f"SilverLayer: Ingesting Spotrac data for {year}")
        files = BronzeLayer.find_files("spotrac_player_contracts", year)
        if not files:
            files = BronzeLayer.find_files("spotrac_player_rankings", year)
            if not files:
                logger.warning(f"No Spotrac files found for {year}")
                return
        
        df = pd.read_csv(files[0])
        df['player_name'] = df['player_name'].apply(clean_doubled_name)
        df = df.rename(columns={
            'total_contract_value_millions': 'cap_hit_millions',
            'guaranteed_money_millions': 'dead_cap_millions',
        })
        
        required_cols = ['cap_hit_millions', 'dead_cap_millions', 'age', 'signing_bonus_millions']
        for col in required_cols:
            if col not in df.columns: df[col] = None

        self.db.execute(f"DELETE FROM silver_spotrac_contracts WHERE year = {year}")
        self.db.execute("INSERT INTO silver_spotrac_contracts BY NAME SELECT DISTINCT * FROM df", {"df": df})

        # Salaries
        sal_files = BronzeLayer.find_files("spotrac_player_salaries", year)
        if sal_files:
            df_sal = pd.read_csv(sal_files[0])
            if 'player_name' in df_sal.columns:
                df_sal['player_name'] = df_sal['player_name'].apply(clean_doubled_name)
            self.db.execute(f"DELETE FROM silver_spotrac_salaries WHERE year = {year}")
            self.db.execute("INSERT INTO silver_spotrac_salaries BY NAME SELECT * FROM df_sal", {"df_sal": df_sal})

    def ingest_pfr(self, year: int):
        logger.info(f"SilverLayer: Ingesting PFR data for {year}")
        files = BronzeLayer.find_files("pfr_game_logs", year)
        if not files:
            pfr_dir = BRONZE_DIR / "pfr" / str(year)
            files = list(pfr_dir.glob(f"game_logs_{year}.csv")) if pfr_dir.exists() else []
            
        if not files: return

        df = pd.read_csv(files[0])
        pfr_col = 'Unnamed: 0_level_0_Player' if 'Unnamed: 0_level_0_Player' in df.columns else 'Player'
        if pfr_col in df.columns:
            df = df.rename(columns={pfr_col: 'player_name'})
            df = df.dropna(subset=['player_name'])
            df['player_name'] = df['player_name'].str.replace('*', '', regex=False).str.replace('+', '', regex=False).str.strip()
        
        tm_col = 'Unnamed: 1_level_0_Tm' if 'Unnamed: 1_level_0_Tm' in df.columns else 'Tm'
        if tm_col in df.columns:
            df = df.rename(columns={tm_col: 'team'})

        self.db.execute(f"DELETE FROM silver_pfr_game_logs WHERE year = {year}")
        self.db.execute("INSERT INTO silver_pfr_game_logs BY NAME SELECT * FROM df", {"df": df})

    def ingest_penalties(self, year: int):
        logger.info(f"SilverLayer: Ingesting Penalties for {year}")
        files = BronzeLayer.find_files("improved_penalties", year)
        if not files: return
        
        df = pd.read_csv(files[-1])
        city_map = {
            "Houston": "HOU", "Dallas": "DAL", "Kansas City": "KC", "Buffalo": "BUF",
            "Pittsburgh": "PIT", "Denver": "DEN", "Baltimore": "BAL", "New Orleans": "NO",
            "New England": "NE", "Washington": "WAS", "Carolina": "CAR", "Atlanta": "ATL",
            "Indianapolis": "IND", "Minnesota": "MIN", "Las Vegas": "LV", "Detroit": "DET",
            "Green Bay": "GB", "Chicago": "CHI", "New York Jets": "NYJ", "New York Giants": "NYG",
            "San Francisco": "SF", "Tampa Bay": "TB", "Seattle": "SEA", "Miami": "MIA",
            "Jacksonville": "JAX", "Cleveland": "CLE", "Cincinnati": "CIN", "Arizona": "ARI",
            "Philadelphia": "PHI", "Tennessee": "TEN", "Los Angeles Rams": "LAR", "Los Angeles Chargers": "LAC"
        }
        df['team'] = df['team_city'].map(city_map)
        
        self.db.execute(f"DELETE FROM silver_penalties WHERE year = {year}")
        self.db.execute("INSERT INTO silver_penalties BY NAME SELECT * FROM df", {"df": df})

    def ingest_team_cap(self):
        logger.info("SilverLayer: Ingesting Team Cap data")
        dead_money_dir = BRONZE_DIR / "dead_money"
        files = list(dead_money_dir.rglob("team_cap_*.csv"))
        if not files: return
        
        dfs = [pd.read_csv(f) for f in files]
        df = pd.concat(dfs)
        self.db.execute("CREATE OR REPLACE TABLE silver_team_cap AS SELECT DISTINCT * FROM df", {"df": df})

    def ingest_others(self):
        logger.info("SilverLayer: Ingesting other static datasets")
        fin_path = BRONZE_DIR / "other" / "finance" / "team_valuations_2024.csv"
        if fin_path.exists():
            load_team_financials(self.db.con, fin_path)
        
        merch_path = BRONZE_DIR / "other" / "merch" / "nflpa_player_sales_2024.csv"
        if merch_path.exists():
            load_player_merch(self.db.con, merch_path)

        draft_file = Path("data/raw/pfr/draft_history.csv")
        if draft_file.exists():
             df_draft = pd.read_csv(draft_file)
             self.db.execute("CREATE OR REPLACE TABLE silver_pfr_draft_history AS SELECT * FROM df_draft", {"df_draft": df_draft})

class GoldLayer:
    """Gold Layer: Aggregating into Feature-Rich Analytics Tables."""
    def __init__(self, db: DBManager):
        self.db = db

    def build_fact_player_efficiency(self):
        logger.info("GoldLayer: Building fact_player_efficiency...")
        
        self.db.execute("""
        CREATE OR REPLACE TABLE fact_player_efficiency AS
        WITH pfr_agg AS (
            SELECT 
                player_name, team, year,
                COUNT(DISTINCT game_url) as games_played,
                SUM(TRY_CAST(Passing_Yds AS FLOAT)) as total_pass_yds,
                SUM(TRY_CAST(Rushing_Yds AS FLOAT)) as total_rush_yds,
                SUM(TRY_CAST(Receiving_Yds AS FLOAT)) as total_rec_yds,
                SUM(TRY_CAST(Passing_TD AS INT) + TRY_CAST(Rushing_TD AS INT) + TRY_CAST(Receiving_TD AS INT)) as total_tds
            FROM silver_pfr_game_logs
            GROUP BY 1, 2, 3
        ),
        penalties_agg AS (
            SELECT 
                player_name_short, team, year,
                SUM(penalty_count) as total_penalty_count,
                SUM(penalty_yards) as total_penalty_yards
            FROM silver_penalties
            GROUP BY 1, 2, 3
        ),
        dedup_contracts AS (
            SELECT 
                CASE 
                    WHEN LENGTH(s.player_name) % 2 = 0 AND SUBSTRING(s.player_name, 1, CAST(LENGTH(s.player_name)/2 AS BIGINT)) = SUBSTRING(s.player_name, CAST(LENGTH(s.player_name)/2 + 1 AS BIGINT))
                    THEN SUBSTRING(s.player_name, 1, CAST(LENGTH(s.player_name)/2 AS BIGINT))
                    ELSE s.player_name 
                END as player_name, 
                s.team, s.year, 
                MAX(s.position) as position,
                SUM(COALESCE(s.cap_hit_millions, r.ranking_cap_hit_millions)) as cap_hit_millions,
                SUM(s.dead_cap_millions) as dead_cap_millions,
                MAX(s.signing_bonus_millions) as signing_bonus_millions,
                MAX(s.age) as age
            FROM silver_spotrac_contracts s
            LEFT JOIN (
                SELECT player_name, year, MAX(ranking_cap_hit_millions) as ranking_cap_hit_millions 
                FROM silver_spotrac_rankings 
                GROUP BY 1, 2
            ) r 
              ON LOWER(TRIM(CAST(s.player_name AS VARCHAR))) = LOWER(TRIM(CAST(r.player_name AS VARCHAR)))
              AND s.year = r.year
            GROUP BY 1, 2, 3
        ),
        salary_dead_cap AS (
            SELECT 
                player_name, team, year,
                MAX(TRY_CAST(REPLACE(REPLACE(REPLACE("dead cap", '$', ''), ',', ''), 'M', '') AS FLOAT)) as salaries_dead_cap_millions
            FROM silver_spotrac_salaries
            GROUP BY 1, 2, 3
        ),
        fact_long_fallback AS (
            SELECT 
                s.*,
                COALESCE(p.games_played, 0) as games_played,
                CAST(COALESCE(p.games_played, 0) AS FLOAT) / 17.0 as availability_rating,
                COALESCE(p.total_pass_yds, 0) as total_pass_yds,
                COALESCE(p.total_rush_yds, 0) as total_rush_yds,
                COALESCE(p.total_rec_yds, 0) as total_rec_yds,
                COALESCE(p.total_tds, 0) as total_tds,
                COALESCE(pen.total_penalty_count, 0) as total_penalty_count,
                COALESCE(pen.total_penalty_yards, 0) as total_penalty_yards
            FROM dedup_contracts s
            LEFT JOIN pfr_agg p ON LOWER(TRIM(CAST(s.player_name AS VARCHAR))) = LOWER(TRIM(CAST(p.player_name AS VARCHAR))) 
                AND s.year = p.year AND s.team = p.team
            LEFT JOIN penalties_agg pen ON s.year = pen.year AND s.team = pen.team
                AND (LOWER(s.player_name) LIKE LOWER(LEFT(pen.player_name_short, 1)) || '%' AND LOWER(s.player_name) LIKE '%' || LOWER(SUBSTRING(pen.player_name_short, 3)))
        )
        SELECT 
            f.*,
            GREATEST(COALESCE(sdc.salaries_dead_cap_millions, 0), f.dead_cap_millions, COALESCE(f.signing_bonus_millions, 0) * 2.0) as potential_dead_cap_millions,
            ( (COALESCE(f.total_tds,0) * 2.0 + (COALESCE(f.total_pass_yds,0) + COALESCE(f.total_rush_yds,0) + COALESCE(f.total_rec_yds,0)) / 100.0) * 1.8 
              - (COALESCE(f.total_penalty_yards,0) / 10.0) ) as fair_market_value
        FROM fact_long_fallback f
        LEFT JOIN salary_dead_cap sdc ON LOWER(TRIM(CAST(f.player_name AS VARCHAR))) = LOWER(TRIM(CAST(sdc.player_name AS VARCHAR))) AND f.year = sdc.year AND LOWER(TRIM(CAST(f.team AS VARCHAR))) = LOWER(TRIM(CAST(sdc.team AS VARCHAR)))
        """)
        logger.info("✓ Gold Layer populated: fact_player_efficiency")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--skip-gold", action="store_true")
    parser.add_argument("--gold-only", action="store_true")
    args = parser.parse_args()

    with DBManager() as db:
        silver = SilverLayer(db)
        gold = GoldLayer(db)

        if not args.gold_only:
            silver.provision_schemas()
            silver.ingest_spotrac(args.year)
            silver.ingest_pfr(args.year)
            silver.ingest_penalties(args.year)
            silver.ingest_team_cap()
            silver.ingest_others()

        if not args.skip_gold or args.gold_only:
            gold.build_fact_player_efficiency()
            
            # ML Enrichment Trigger
            try:
                from src.inference import InferenceEngine
                engine = InferenceEngine(db.db_path, model_dir="/tmp/models")
                engine.enrich_gold_layer()
            except Exception as e:
                logger.warning(f"ML Enrichment failed: {e}")

if __name__ == "__main__":
    main()
