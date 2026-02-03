
import duckdb
import pandas as pd
import glob
import os
import re
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = "data/nfl_data.db"
RAW_DIR = Path("data/raw")

def clean_doubled_name(name):
    if not isinstance(name, str): return name
    parts = name.strip().split()
    if len(parts) < 2: return name
    # Case 1: "Peterson Patrick Peterson"
    if len(parts) >= 3 and parts[0] == parts[-1]:
        return " ".join(parts[1:])
    # Case 2: "Patrick Peterson Patrick Peterson"
    mid = len(parts) // 2
    if len(parts) % 2 == 0:
        if parts[:mid] == parts[mid:]:
            return " ".join(parts[:mid])
    return name

def ingest_spotrac():
    con = duckdb.connect(DB_PATH)
    logger.info("ingesting Spotrac Contracts...")
    
    # Ingest Contracts
    files = glob.glob("data/raw/spotrac_player_contracts_*.csv")
    all_dfs = []
    for f in files:
        df = pd.read_csv(f)
        df['player_name'] = df['player_name'].apply(clean_doubled_name)
        all_dfs.append(df)
    
    if all_dfs:
        df_contracts = pd.concat(all_dfs, ignore_index=True)
        con.execute("CREATE OR REPLACE TABLE silver_spotrac_contracts AS SELECT * FROM df_contracts")
        logger.info(f"✓ Loaded {len(df_contracts)} contracts into silver_spotrac_contracts")

    # Ingest Dead Money (Salaries)
    logger.info("ingesting Spotrac Dead Money...")
    sal_files = glob.glob("data/raw/spotrac_player_salaries_*.csv")
    all_sal_dfs = []
    for f in sal_files:
        df = pd.read_csv(f)
        # Salaries page sometimes has different name format or is team-summary
        if 'player_name' in df.columns:
            df['player_name'] = df['player_name'].apply(clean_doubled_name)
        all_sal_dfs.append(df)
    
    if all_sal_dfs:
        df_salaries = pd.concat(all_sal_dfs, ignore_index=True)
        con.execute("CREATE OR REPLACE TABLE silver_spotrac_salaries AS SELECT * FROM df_salaries")
        logger.info(f"✓ Loaded {len(df_salaries)} dead money records into silver_spotrac_salaries")
        
    # Ingest Rankings (as a source for Cap Hits)
    logger.info("ingesting Spotrac Rankings...")
    rank_files = glob.glob("data/raw/spotrac_player_rankings_*.csv")
    all_rank_dfs = []
    for f in rank_files:
        df = pd.read_csv(f)
        df['player_name'] = df['player_name'].apply(clean_doubled_name)
        # In rankings, 'total_contract_value_millions' is often the cap hit for that specific year
        df = df.rename(columns={'total_contract_value_millions': 'ranking_cap_hit_millions'})
        all_rank_dfs.append(df)
        
    if all_rank_dfs:
        df_rankings = pd.concat(all_rank_dfs, ignore_index=True)
        con.execute("CREATE OR REPLACE TABLE silver_spotrac_rankings AS SELECT * FROM df_rankings")
        logger.info(f"✓ Loaded {len(df_rankings)} rankings into silver_spotrac_rankings")

    logger.info("ingesting Player Metadata...")
    meta_path = RAW_DIR / "player_metadata.csv"
    if meta_path.exists():
        df_meta = pd.read_csv(meta_path)
        con.execute("CREATE OR REPLACE TABLE silver_player_metadata AS SELECT * FROM df_meta")
        logger.info(f"✓ Loaded {len(df_meta)} metadata records into silver_player_metadata")
    else:
        con.execute("CREATE TABLE IF NOT EXISTS silver_player_metadata (full_name VARCHAR, birth_date VARCHAR, college VARCHAR, draft_round INT, draft_pick INT, experience_years VARCHAR)")

    con.close()

def ingest_pfr():
    con = duckdb.connect(DB_PATH)
    logger.info("ingesting PFR Game Logs...")
    
    files = glob.glob("data/raw/pfr/game_logs_*.csv")
    all_dfs = []
    for f in files:
        df = pd.read_csv(f)
        # Normalize name column from multi-index artifact
        pfr_col = 'Unnamed: 0_level_0_Player' if 'Unnamed: 0_level_0_Player' in df.columns else 'Player'
        if pfr_col in df.columns:
            df = df.rename(columns={pfr_col: 'player_name'})
            # Clean PFR suffixes (*, +)
            df['player_name'] = df['player_name'].str.replace('*', '', regex=False).str.replace('+', '', regex=False).str.strip()
        all_dfs.append(df)
        
    if all_dfs:
        df_pfr = pd.concat(all_dfs, ignore_index=True)
        con.execute("CREATE OR REPLACE TABLE silver_pfr_game_logs AS SELECT * FROM df_pfr")
        logger.info(f"✓ Loaded {len(df_pfr)} game logs into silver_pfr_game_logs")
        
    con.close()

def create_gold_layer():
    con = duckdb.connect(DB_PATH)
    # 6. PFR Draft History
    logger.info("ingesting PFR Draft History...")
    draft_file = Path("data/raw/pfr/draft_history.csv")
    if draft_file.exists():
        con.execute("CREATE OR REPLACE TABLE silver_pfr_draft_history AS SELECT * FROM read_csv_auto('data/raw/pfr/draft_history.csv')")
        count = con.execute("SELECT COUNT(*) FROM silver_pfr_draft_history").fetchone()[0]
        logger.info(f"✓ Loaded {count} draft picks into silver_pfr_draft_history")
    else:
        logger.warning("No draft history file found at data/raw/pfr/draft_history.csv")

    logger.info("Building Gold Layer: fact_player_efficiency...")
    
    # This SQL joins financials with aggregated performance
    # We use a simple aggregate for the proxy for now (to be refined)
    con.execute("""
    CREATE OR REPLACE TABLE fact_player_efficiency AS
    WITH pfr_agg AS (
        SELECT 
            player_name,
            year,
            COUNT(*) as games_played,
            SUM(TRY_CAST(Passing_Yds AS FLOAT)) as total_pass_yds,
            SUM(TRY_CAST(Rushing_Yds AS FLOAT)) as total_rush_yds,
            SUM(TRY_CAST(Receiving_Yds AS FLOAT)) as total_rec_yds,
            SUM(TRY_CAST(Passing_TD AS INT) + TRY_CAST(Rushing_TD AS INT) + TRY_CAST(Receiving_TD AS INT)) as total_tds
        FROM silver_pfr_game_logs
        GROUP BY 1, 2
    ),
    dedup_contracts AS (
        SELECT 
            s.player_name, s.team, s.year, s.position,
            MAX(COALESCE(s.cap_hit_millions, r.ranking_cap_hit_millions)) as cap_hit_millions,
            MAX(s.dead_cap_millions) as dead_cap_millions,
            MAX(s.signing_bonus_millions) as signing_bonus_millions,
            MAX(s.guaranteed_money_millions) as guaranteed_money_millions,
            MAX(s.age) as age
        FROM silver_spotrac_contracts s
        LEFT JOIN silver_spotrac_rankings r 
          ON LOWER(TRIM(CAST(s.player_name AS VARCHAR))) = LOWER(TRIM(CAST(r.player_name AS VARCHAR)))
          AND s.year = r.year
        GROUP BY 1, 2, 3, 4
    ),
    salary_dead_cap AS (
        SELECT 
            player_name, team, year,
            MAX(TRY_CAST(REPLACE(REPLACE(REPLACE("dead cap", '$', ''), ',', ''), 'M', '') AS FLOAT)) as salaries_dead_cap_millions
        FROM silver_spotrac_salaries
        GROUP BY 1, 2, 3
    ),
    age_fix AS (
        SELECT 
            s.player_name, 
            s.year,
            COALESCE(
                TRY_CAST(s.year - EXTRACT(year FROM CAST(m.birth_date AS DATE)) AS INT),
                MAX(s.age) FILTER(WHERE s.age IS NOT NULL) OVER (PARTITION BY s.player_name) - (MAX(s.year) FILTER(WHERE s.age IS NOT NULL) OVER (PARTITION BY s.player_name) - s.year)
            ) as corrected_age,
            m.college,
            m.draft_round,
            m.draft_pick,
            m.experience_years
        FROM silver_spotrac_contracts s
        LEFT JOIN silver_player_metadata m 
          ON LOWER(TRIM(CAST(s.player_name AS VARCHAR))) = LOWER(TRIM(CAST(m.full_name AS VARCHAR)))
    ),
    age_risk_theshold AS (
        SELECT 
            s.player_name, s.year,
            CASE 
                WHEN s.position = 'QB' THEN 35
                WHEN s.position = 'RB' THEN 28
                WHEN s.position IN ('WR', 'CB', 'FS', 'SS', 'DB') THEN 30
                WHEN s.position IN ('LT', 'RT', 'G', 'C', 'DE', 'DT', 'DL', 'LB', 'OLB', 'ILB') THEN 32
                ELSE 30
            END as threshold
        FROM dedup_contracts s
    ),
    pfr_2024 AS (
        SELECT player_name, games_played, total_pass_yds, total_rush_yds, total_rec_yds, total_tds
        FROM pfr_agg WHERE year = 2024
    ),
    fact_long_fallback AS (
        SELECT 
            s.*,
            -- Core Performance Metrics with 2024 Fallback
            COALESCE(p.games_played, p24.games_played) as games_played,
            COALESCE(p.total_pass_yds, p24.total_pass_yds) as total_pass_yds,
            COALESCE(p.total_rush_yds, p24.total_rush_yds) as total_rush_yds,
            COALESCE(p.total_rec_yds, p24.total_rec_yds) as total_rec_yds,
            COALESCE(p.total_tds, p24.total_tds) as total_tds,
            af.corrected_age as age_final
        FROM dedup_contracts s
        LEFT JOIN pfr_agg p ON LOWER(TRIM(CAST(s.player_name AS VARCHAR))) = LOWER(TRIM(CAST(p.player_name AS VARCHAR))) AND s.year = p.year
        LEFT JOIN pfr_2024 p24 ON LOWER(TRIM(CAST(s.player_name AS VARCHAR))) = LOWER(TRIM(CAST(p24.player_name AS VARCHAR)))
        LEFT JOIN age_fix af ON s.player_name = af.player_name AND s.year = af.year
    )
    SELECT 
        f.player_name, f.team, f.position, f.year, f.cap_hit_millions,
        GREATEST(COALESCE(sdc.salaries_dead_cap_millions, 0), f.dead_cap_millions, COALESCE(f.signing_bonus_millions, 0) * 2.0, COALESCE(f.guaranteed_money_millions, 0)) as potential_dead_cap_millions,
        f.age_final as age,
        af.college, af.draft_round, af.draft_pick, af.experience_years,
        f.games_played, f.total_pass_yds, f.total_rush_yds, f.total_rec_yds, f.total_tds,
        
        -- Efficiency Logic (Using Fallback Stats)
        ( (COALESCE(f.total_tds,0) * 2.0 + (COALESCE(f.total_pass_yds,0) + COALESCE(f.total_rush_yds,0) + COALESCE(f.total_rec_yds,0)) / 100.0) * 1.8 ) as fair_market_value,
        f.cap_hit_millions - ( (COALESCE(f.total_tds,0) * 2.0 + (COALESCE(f.total_pass_yds,0) + COALESCE(f.total_rush_yds,0) + COALESCE(f.total_rec_yds,0)) / 100.0) * 1.8 ) as ied_overpayment,
        ( 1.0 / (1.0 + exp(-(f.age_final - ar.threshold))) ) * GREATEST(COALESCE(sdc.salaries_dead_cap_millions, 0), f.dead_cap_millions, COALESCE(f.signing_bonus_millions, 0) * 2.0, COALESCE(f.guaranteed_money_millions, 0)) as edce_risk,
        
        -- Placeholders
        5.0 as narrative_risk_score,
        10 as sentiment_volume,
        
        ( (COALESCE(f.total_tds,0) * 6 + (COALESCE(f.total_pass_yds,0) + COALESCE(f.total_rush_yds,0) + COALESCE(f.total_rec_yds,0)) / 20.0) / NULLIF(f.cap_hit_millions, 0) ) as value_metric_proxy
        
    FROM fact_long_fallback f
    LEFT JOIN age_risk_theshold ar ON f.player_name = ar.player_name AND f.year = ar.year
    LEFT JOIN salary_dead_cap sdc ON LOWER(TRIM(CAST(f.player_name AS VARCHAR))) = LOWER(TRIM(CAST(sdc.player_name AS VARCHAR))) AND f.year = sdc.year
    LEFT JOIN age_fix af ON f.player_name = af.player_name AND f.year = af.year
    """)
    
    logger.info("✓ Gold Layer created: fact_player_efficiency")
    con.close()

if __name__ == "__main__":
    ingest_spotrac()
    ingest_pfr()
    create_gold_layer()
