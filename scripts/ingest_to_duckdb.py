
import duckdb
import pandas as pd
import glob
import os
import re
from pathlib import Path
import logging
import sys

# Ensure project root is in path
sys.path.append(str(Path(__file__).parent.parent))
from src.financial_ingestion import load_team_financials, load_player_merch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Medallion Architecture: Silver/Gold layer database
from src.config_loader import get_db_path, get_bronze_dir

DB_PATH = get_db_path()
# Medallion Architecture: Bronze layer is the source of truth
BRONZE_DIR = get_bronze_dir()

def clean_doubled_name(name):
    if not isinstance(name, str): return name
    # Case 1: "Peterson Patrick Peterson" (Last First Last)
    parts = name.strip().split()
    if len(parts) >= 3 and parts[0] == parts[-1]:
        return " ".join(parts[1:])

    # Case 2: No spaces "Kyler MurrayKyler Murray"
    mid_idx = len(name) // 2
    if len(name) % 2 == 0:
        first_half = name[:mid_idx]
        second_half = name[mid_idx:]
        if first_half == second_half:
            return first_half

    # Case 3: "Patrick Peterson Patrick Peterson"
    parts = name.strip().split()
    if len(parts) < 2: return name
    mid = len(parts) // 2
    if len(parts) % 2 == 0:
        if parts[:mid] == parts[mid:]:
            return " ".join(parts[:mid])
    return name



def find_targeted_files(pattern_name: str, year: int, week: int = None):
    """
    Finds the specific file for a given pattern based on parameters.
    First Principle: Explicit Determinism.
    Input: data/raw/{year}/week_{week}_{ts}/
    """
    if year is None:
        raise ValueError("Year parameter is required for explicit ingestion.")
        
    # Look in source-specific subdirectories under bronze
    # Try spotrac first for contract/ranking files
    source_dirs = ['spotrac', 'pfr', 'penalties', 'dead_money']
    
    for source in source_dirs:
        year_dir = BRONZE_DIR / source / str(year)
    
        # Check if files exist in this source/year directory
        if year_dir.exists():
            files = list(year_dir.glob(f"{pattern_name}*.csv"))
            if files:
                files.sort()
                logger.info(f"Found {pattern_name} in {year_dir}: {files[-1].name}")
                return [files[-1]]
             
    # No files found in any source directory
    logger.warning(f"No files found for {pattern_name} ({year}) in bronze layer")
    return []

def ingest_spotrac(year: int, week: int):
    con = duckdb.connect(DB_PATH)
    logger.info(f"Ingesting Spotrac (Year={year}, Week={week}) - Upsert Mode")
    
    files = find_targeted_files("spotrac_player_contracts", year, week)
    
    # FALLBACK: Use rankings data as contract source if no contract file exists
    use_rankings_fallback = False
    if not files:
        files = find_targeted_files("spotrac_player_rankings", year, week)
        if files:
            use_rankings_fallback = True
            logger.info(f"Using rankings as fallback contract source for {year}")
        else:
            logger.warning("Skipping Spotrac ingest: No file found.")
            con.close()
            return

    # Ingest Temporary Table
    df = pd.read_csv(files[0])
    df['player_name'] = df['player_name'].apply(clean_doubled_name)
    
    # SCHEMA NORMALIZATION: Handle column variations across years
    # Rename common variants to standard names
    column_mappings = {
        'total_contract_value_millions': 'cap_hit_millions',
        'guaranteed_money_millions': 'dead_cap_millions',
    }
    df = df.rename(columns=column_mappings)
    
    # Ensure required columns exist with defaults
    required_cols = ['cap_hit_millions', 'dead_cap_millions', 'age', 'signing_bonus_millions']
    for col in required_cols:
        if col not in df.columns:
            df[col] = None
    
    # UPSERT Logic: Contracts
    con.execute("CREATE TABLE IF NOT EXISTS silver_spotrac_contracts AS SELECT * FROM df LIMIT 0")
    # Schema evolution: add any new columns from DF
    existing_cols = con.execute("PRAGMA table_info('silver_spotrac_contracts')").df()['name'].tolist()
    for col in df.columns:
        if col not in existing_cols:
            logger.info(f"Adding column {col} to silver_spotrac_contracts")
            con.execute(f'ALTER TABLE silver_spotrac_contracts ADD COLUMN "{col}" VARCHAR')
    con.execute(f"DELETE FROM silver_spotrac_contracts WHERE year = {year}")
    # Enforce uniqueness at ingestion to prevent row explosion
    con.execute("INSERT INTO silver_spotrac_contracts BY NAME SELECT DISTINCT * FROM df")
    count = con.execute(f"SELECT COUNT(*) FROM silver_spotrac_contracts WHERE year = {year}").fetchone()[0]
    logger.info(f"✓ Upserted {count} rows into silver_spotrac_contracts for {year}")


    # Dead Money
    sal_files = find_targeted_files("spotrac_player_salaries", year, week)
    if sal_files:
        df_sal = pd.read_csv(sal_files[0])
        if 'player_name' in df_sal.columns:
            df_sal['player_name'] = df_sal['player_name'].apply(clean_doubled_name)
            
        con.execute("CREATE TABLE IF NOT EXISTS silver_spotrac_salaries AS SELECT * FROM df_sal LIMIT 0")
        con.execute(f"DELETE FROM silver_spotrac_salaries WHERE year = {year}")
        try:
             con.execute("INSERT INTO silver_spotrac_salaries BY NAME SELECT * FROM df_sal")
        except Exception as e:
             logger.warning(f"Schema mismatch in salaries: {e}")
        logger.info(f"✓ Upserted rows into silver_spotrac_salaries for {year}")
    else:
        # Hardened Schema Provisioning: Ensure table exists for gold layer join
        con.execute("""
            CREATE TABLE IF NOT EXISTS silver_spotrac_salaries (
                player_name VARCHAR,
                team VARCHAR,
                year INTEGER,
                "dead cap" VARCHAR
            )
        """)
        logger.info("✓ Provisioned baseline silver_spotrac_salaries schema (Empty).")

    # Rankings
    rank_files = find_targeted_files("spotrac_player_rankings", year, week)
    if rank_files:
        df_rank = pd.read_csv(rank_files[0])
        df_rank['player_name'] = df_rank['player_name'].apply(clean_doubled_name)
        df_rank = df_rank.rename(columns={'total_contract_value_millions': 'ranking_cap_hit_millions'})

        con.execute("CREATE TABLE IF NOT EXISTS silver_spotrac_rankings AS SELECT * FROM df_rank LIMIT 0")
        con.execute(f"DELETE FROM silver_spotrac_rankings WHERE year = {year}")
        con.execute("INSERT INTO silver_spotrac_rankings BY NAME SELECT * FROM df_rank")
        logger.info(f"✓ Upserted rows into silver_spotrac_rankings for {year}")
        
    con.close()

def ingest_pfr(year: int, week: int):
    con = duckdb.connect(DB_PATH)
    logger.info(f"Ingesting PFR (Year={year}, Week={week}) - Upsert Mode")
    
    files = find_targeted_files("pfr_game_logs", year, week)
    if not files:
        # PFR files are in data/bronze/pfr/{year}/
        pfr_dir = BRONZE_DIR / "pfr" / str(year)
        files = list(pfr_dir.glob(f"game_logs_{year}.csv")) if pfr_dir.exists() else []
        if files: logger.info(f"Using legacy PFR file: {files[0]}")
        
    if not files:
        logger.warning("Skipping PFR ingest: No file found.")
        con.close()
        return

    df = pd.read_csv(files[0])
    pfr_col = 'Unnamed: 0_level_0_Player' if 'Unnamed: 0_level_0_Player' in df.columns else 'Player'
    if pfr_col in df.columns:
        df = df.rename(columns={pfr_col: 'player_name'})
        df = df.dropna(subset=['player_name'])
        df['player_name'] = df['player_name'].str.replace('*', '', regex=False).str.replace('+', '', regex=False).str.strip()
    
    # Standardize Team column
    tm_col = 'Unnamed: 1_level_0_Tm' if 'Unnamed: 1_level_0_Tm' in df.columns else 'Tm'
    if tm_col in df.columns:
        df = df.rename(columns={tm_col: 'team'})
    
    # UPSERT Logic
    con.execute("CREATE TABLE IF NOT EXISTS silver_pfr_game_logs AS SELECT * FROM df LIMIT 0")
    # Ensure team column exists for older table versions
    con.execute("ALTER TABLE silver_pfr_game_logs ADD COLUMN IF NOT EXISTS team VARCHAR")
    con.execute(f"DELETE FROM silver_pfr_game_logs WHERE year = {year}")
    con.execute("INSERT INTO silver_pfr_game_logs BY NAME SELECT * FROM df")
    logger.info(f"✓ Upserted rows into silver_pfr_game_logs for {year}")
    
    con.close()

def ingest_financials():
    # Financials are static for now, no partition logic needed yet
    con = duckdb.connect(DB_PATH)
    fin_path = BRONZE_DIR / "other" / "finance" / "team_valuations_2024.csv"
    if fin_path.exists():
        load_team_financials(con, fin_path)
    else:
        logger.warning(f"Financial data not found at {fin_path}")
    merch_path = BRONZE_DIR / "other" / "merch" / "nflpa_player_sales_2024.csv"
    if merch_path.exists():
        load_player_merch(con, merch_path)
    else:
        logger.warning(f"Merch data not found at {merch_path}")
    con.close()


def ingest_penalties(year: int):
    con = duckdb.connect(DB_PATH)
    logger.info(f"Ingesting Penalties (Year={year})")
    
    # Penalties are in data/bronze/penalties/{year}/
    penalties_dir = BRONZE_DIR / "penalties" / str(year)
    files = list(penalties_dir.glob(f"improved_penalties_{year}_*.csv")) if penalties_dir.exists() else []
    if not files:
        logger.warning(f"No penalty files found for {year}")
        con.close()
        return
        
    df = pd.read_csv(files[-1])
    
    # City mapping to NFL abbreviations
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
    
    con.execute("CREATE TABLE IF NOT EXISTS silver_penalties AS SELECT * FROM df LIMIT 0")
    # Schema evolution: Ensure all columns in DF exist in table
    existing_cols = con.execute("PRAGMA table_info('silver_penalties')").df()['name'].tolist()
    for col in df.columns:
        if col not in existing_cols:
            logger.info(f"Adding missing column {col} to silver_penalties")
            # Quote column name to support spaces (e.g., 'Total Flags')
            con.execute(f'ALTER TABLE silver_penalties ADD COLUMN "{col}" VARCHAR')
    con.execute(f"DELETE FROM silver_penalties WHERE year = {year}")
    con.execute("INSERT INTO silver_penalties BY NAME SELECT * FROM df")
    logger.info(f"✓ Upserted rows into silver_penalties for {year}")
    con.close()

def ingest_team_cap():
    con = duckdb.connect(DB_PATH)
    logger.info("Ingesting Team Cap & Record data (Historical)...")
    # Team cap files are in data/bronze/dead_money/{year}/
    dead_money_dir = BRONZE_DIR / "dead_money"
    files = list(dead_money_dir.rglob("team_cap_*.csv"))
    if not files:
        logger.warning("No team cap files found in data/bronze/dead_money")
        con.close()
        return
        
    # Combine all years
    dfs = []
    for f in files:
        dfs.append(pd.read_csv(f))
    
    if dfs:
        df = pd.concat(dfs)
        con.execute("CREATE OR REPLACE TABLE silver_team_cap AS SELECT DISTINCT * FROM df")
        logger.info(f"✓ Loaded {len(df)} team-year records (deduplicated) into silver_team_cap")
    
    con.close()

def provision_silver_schemas(con):
    logger.info("Provisioning silver schemas to ensure pipeline stability...")
    con.execute("CREATE TABLE IF NOT EXISTS silver_pfr_game_logs (player_name VARCHAR, team VARCHAR, year INTEGER, game_url VARCHAR, Passing_Yds VARCHAR, Rushing_Yds VARCHAR, Receiving_Yds VARCHAR, Passing_TD VARCHAR, Rushing_TD VARCHAR, Receiving_TD VARCHAR)")
    con.execute("CREATE TABLE IF NOT EXISTS silver_penalties (player_name_short VARCHAR, team VARCHAR, year INTEGER, penalty_count INTEGER, penalty_yards INTEGER)")
    con.execute("CREATE TABLE IF NOT EXISTS silver_spotrac_contracts (player_name VARCHAR, team VARCHAR, year INTEGER, position VARCHAR, cap_hit_millions FLOAT, dead_cap_millions FLOAT, signing_bonus_millions FLOAT, age INTEGER)")
    con.execute("CREATE TABLE IF NOT EXISTS silver_spotrac_rankings (player_name VARCHAR, year INTEGER, ranking_cap_hit_millions FLOAT)")
    con.execute("CREATE TABLE IF NOT EXISTS silver_team_cap (team VARCHAR, year INTEGER, win_pct FLOAT)")
    con.execute("CREATE TABLE IF NOT EXISTS silver_player_metadata (full_name VARCHAR, birth_date VARCHAR, college VARCHAR, draft_round INTEGER, draft_pick INTEGER, experience_years INTEGER)")
    con.execute("CREATE TABLE IF NOT EXISTS silver_player_merch (Player VARCHAR, Rank INTEGER)")
    con.execute("CREATE TABLE IF NOT EXISTS silver_team_finance (Team VARCHAR, Year INTEGER, Revenue_M FLOAT, OperatingIncome_M FLOAT)")
    con.execute("CREATE TABLE IF NOT EXISTS silver_spotrac_salaries (player_name VARCHAR, team VARCHAR, year INTEGER, \"dead cap\" VARCHAR)")

def create_gold_layer():
    con = duckdb.connect(DB_PATH)
    
    # Ensure tables exist before querying
    provision_silver_schemas(con)
    
    # 6. PFR Draft History
    logger.info("ingesting PFR Draft History...")
    draft_file = Path("data/raw/pfr/draft_history.csv")
    if draft_file.exists():
        # Ensure table exists first to avoid schema mismatch on replace
        con.execute("CREATE TABLE IF NOT EXISTS silver_pfr_draft_history AS SELECT * FROM read_csv_auto('data/raw/pfr/draft_history.csv') LIMIT 0")
        con.execute("CREATE OR REPLACE TABLE silver_pfr_draft_history AS SELECT * FROM read_csv_auto('data/raw/pfr/draft_history.csv')")
        count = con.execute("SELECT COUNT(*) FROM silver_pfr_draft_history").fetchone()[0]
        logger.info(f"✓ Loaded {count} draft picks into silver_pfr_draft_history")
    else:
        con.execute("CREATE TABLE IF NOT EXISTS silver_pfr_draft_history (player_name VARCHAR, team VARCHAR, year INTEGER, draft_round INTEGER, draft_pick INTEGER)")
        logger.warning("No draft history file found at data/raw/pfr/draft_history.csv")

    logger.info("Building Gold Layer: fact_player_efficiency...")
    
    con.execute("""
    CREATE OR REPLACE TABLE fact_player_efficiency AS
    WITH pfr_agg AS (
        SELECT 
            player_name,
            team,
            year,
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
    age_fix AS (
        SELECT 
            CASE 
                WHEN LENGTH(s.player_name) % 2 = 0 AND SUBSTRING(s.player_name, 1, CAST(LENGTH(s.player_name)/2 AS BIGINT)) = SUBSTRING(s.player_name, CAST(LENGTH(s.player_name)/2 + 1 AS BIGINT))
                THEN SUBSTRING(s.player_name, 1, CAST(LENGTH(s.player_name)/2 AS BIGINT))
                ELSE s.player_name 
            END as player_name, 
            s.year,
            MAX(COALESCE(
                TRY_CAST(s.year - EXTRACT(year FROM CAST(m.birth_date AS DATE)) AS INT),
                MAX_AGE_HIST
            )) as corrected_age,
            MAX(m.college) as college,
            MAX(m.draft_round) as draft_round,
            MAX(m.draft_pick) as draft_pick,
            MAX(m.experience_years) as experience_years
        FROM (
            SELECT 
                player_name, year, 
                MAX(age) OVER (PARTITION BY player_name) - (MAX(year) OVER (PARTITION BY player_name) - year) as MAX_AGE_HIST
            FROM silver_spotrac_contracts
        ) s
        LEFT JOIN (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(full_name)) ORDER BY birth_date DESC) as rn 
            FROM silver_player_metadata
        ) m ON LOWER(TRIM(CAST(s.player_name AS VARCHAR))) = LOWER(TRIM(CAST(m.full_name AS VARCHAR))) AND m.rn = 1
        GROUP BY 1, 2
    ),
    age_risk_theshold AS (
        SELECT 
            player_name, year,
            MAX(CASE 
                WHEN position = 'QB' THEN 35
                WHEN position = 'RB' THEN 28
                WHEN position IN ('WR', 'CB', 'FS', 'SS', 'DB') THEN 30
                WHEN position IN ('LT', 'RT', 'G', 'C', 'DE', 'DT', 'DL', 'LB', 'OLB', 'ILB') THEN 32
                ELSE 30
            END) as threshold
        FROM dedup_contracts
        GROUP BY 1, 2
    ),
    -- Join all parts
    fact_long_fallback AS (
        SELECT 
            s.*,
            COALESCE(p.games_played, 0) as games_played,
            CAST(COALESCE(p.games_played, 0) AS FLOAT) / 17.0 as availability_rating,
            COALESCE(p.total_pass_yds, 0) as total_pass_yds,
            COALESCE(p.total_rush_yds, 0) as total_rush_yds,
            COALESCE(p.total_rec_yds, 0) as total_rec_yds,
            COALESCE(p.total_tds, 0) as total_tds,
            af.corrected_age as age_final,
            af.college, af.draft_round, af.draft_pick, af.experience_years,
            (s.cap_hit_millions * 1.08) as future_cap_projection_2026,
            
            -- Penalties
            COALESCE(pen.total_penalty_count, 0) as total_penalty_count,
            COALESCE(pen.total_penalty_yards, 0) as total_penalty_yards,
            
            -- Financial Lift
            COALESCE(pm.popularity_score, 0) as popularity_score,
            COALESCE(pm.Rank, 999) as merch_rank,
            tc.Revenue_M as team_revenue,
            tc.OperatingIncome_M as team_op_income
        FROM dedup_contracts s
        LEFT JOIN pfr_agg p ON LOWER(TRIM(CAST(s.player_name AS VARCHAR))) = LOWER(TRIM(CAST(p.player_name AS VARCHAR))) 
            AND s.year = p.year AND s.team = p.team
        -- Fuzzy join for penalties (Short name L.Tunsil starts with first letter + contains last name)
        LEFT JOIN penalties_agg pen ON s.year = pen.year AND s.team = pen.team
            AND (LOWER(s.player_name) LIKE LOWER(LEFT(pen.player_name_short, 1)) || '%' AND LOWER(s.player_name) LIKE '%' || LOWER(SUBSTRING(pen.player_name_short, 3)))
        LEFT JOIN age_fix af ON s.player_name = af.player_name AND s.year = af.year
        LEFT JOIN (
            SELECT Player, MIN(Rank) as Rank, MAX(51 - Rank) as popularity_score FROM silver_player_merch GROUP BY 1
        ) pm ON LOWER(TRIM(CAST(s.player_name AS VARCHAR))) = LOWER(TRIM(CAST(pm.Player AS VARCHAR)))
        LEFT JOIN (
            SELECT Team, MAX(Revenue_M) as Revenue_M, MAX(OperatingIncome_M) as OperatingIncome_M FROM silver_team_finance GROUP BY 1
        ) tc ON LOWER(TRIM(CAST(s.team AS VARCHAR))) = LOWER(TRIM(CAST(tc.Team AS VARCHAR)))
        LEFT JOIN (
            SELECT team, year, MAX(win_pct) as win_pct FROM silver_team_cap GROUP BY 1, 2
        ) stc ON s.team = stc.team AND s.year = stc.year
    )
    SELECT 
        f.player_name, f.team, f.position, f.year, f.cap_hit_millions,
        GREATEST(COALESCE(sdc.salaries_dead_cap_millions, 0), f.dead_cap_millions, COALESCE(f.signing_bonus_millions, 0) * 2.0) as potential_dead_cap_millions,
        f.age_final as age,
        f.college, f.draft_round, f.draft_pick, f.experience_years,
        f.games_played, f.availability_rating, f.total_pass_yds, f.total_rush_yds, f.total_rec_yds, f.total_tds,
        f.total_penalty_count, f.total_penalty_yards,
        COALESCE(stc.win_pct, 0.500) as win_pct,
        f.future_cap_projection_2026,
        
        -- Efficiency Logic (Now including penalties as a negative weight)
        ( (COALESCE(f.total_tds,0) * 2.0 + (COALESCE(f.total_pass_yds,0) + COALESCE(f.total_rush_yds,0) + COALESCE(f.total_rec_yds,0)) / 100.0) * 1.8 
          - (COALESCE(f.total_penalty_yards,0) / 10.0) ) as fair_market_value,
          
        f.cap_hit_millions - ( (COALESCE(f.total_tds,0) * 2.0 + (COALESCE(f.total_pass_yds,0) + COALESCE(f.total_rush_yds,0) + COALESCE(f.total_rec_yds,0)) / 100.0) * 1.8 
          - (COALESCE(f.total_penalty_yards,0) / 10.0) ) as ied_overpayment,
          
        ( (1.0 / (1.0 + exp(-(f.age_final - ar.threshold)))) * potential_dead_cap_millions ) + (COALESCE(f.total_penalty_yards,0) / 50.0) as edce_risk,
        
        -- Calculated Value Metrics
        ( (COALESCE(f.total_tds,0) * 6 + (COALESCE(f.total_pass_yds,0) + COALESCE(f.total_rush_yds,0) + COALESCE(f.total_rec_yds,0)) / 20.0) / NULLIF(f.cap_hit_millions, 0) ) as value_metric_proxy,

        -- Commercial / Off-Field Impact Metrics (First-Party Principles)
        f.popularity_score, 
        f.merch_rank, 
        (f.popularity_score * 0.2) as estimated_merch_revenue_M,
        (CASE WHEN f.merch_rank <= 10 THEN 5.0 WHEN f.merch_rank <= 50 THEN 2.0 ELSE 0 END) as ticket_premium_lift_M,
        ( (f.popularity_score * 0.2) + (CASE WHEN f.merch_rank <= 10 THEN 5.0 WHEN f.merch_rank <= 50 THEN 2.0 ELSE 0 END) ) as financial_lift_total_M,
        
        f.team_revenue, 
        f.team_op_income,
        
        -- Combined Efficiency Score (Football ROI + Financial ROI)
        ( ( (COALESCE(f.total_tds,0) * 6 + (COALESCE(f.total_pass_yds,0) + COALESCE(f.total_rush_yds,0) + COALESCE(f.total_rec_yds,0)) / 20.0) / NULLIF(f.cap_hit_millions, 0) ) + ( ( (f.popularity_score * 0.2) + (CASE WHEN f.merch_rank <= 10 THEN 5.0 WHEN f.merch_rank <= 50 THEN 2.0 ELSE 0 END) ) / NULLIF(f.cap_hit_millions, 0) ) ) as combined_roi_score,
        ( ( (COALESCE(f.total_tds,0) * 6 + (COALESCE(f.total_pass_yds,0) + COALESCE(f.total_rush_yds,0) + COALESCE(f.total_rec_yds,0)) / 20.0) / NULLIF(f.cap_hit_millions, 0) )) as cap_roi_score
        
    FROM fact_long_fallback f
    LEFT JOIN age_risk_theshold ar ON f.player_name = ar.player_name AND f.year = ar.year
    LEFT JOIN salary_dead_cap sdc ON LOWER(TRIM(CAST(f.player_name AS VARCHAR))) = LOWER(TRIM(CAST(sdc.player_name AS VARCHAR))) AND f.year = sdc.year AND LOWER(TRIM(CAST(f.team AS VARCHAR))) = LOWER(TRIM(CAST(sdc.team AS VARCHAR)))
    LEFT JOIN (
        SELECT team, year, MAX(win_pct) as win_pct FROM silver_team_cap GROUP BY 1, 2
    ) stc ON f.team = stc.team AND f.year = stc.year
    """)
    
    logger.info("✓ Gold Layer created: fact_player_efficiency (Penalty-Aware)")
    con.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Specific year to ingest (required)", required=True)
    parser.add_argument("--week", type=int, help="Specific week (optional)", default=None)
    parser.add_argument("--skip-gold", action="store_true", help="Skip Gold Layer Building")
    parser.add_argument("--gold-only", action="store_true", help="Only build Gold Layer")
    args = parser.parse_args()

    if not args.gold_only:
        ingest_spotrac(year=args.year, week=args.week)
        ingest_pfr(year=args.year, week=args.week)
        ingest_penalties(year=args.year)
        ingest_financials()
        ingest_team_cap()
    
    if not args.skip_gold or args.gold_only:
        create_gold_layer()
        
        # NEW: Phase 2 Strategic Upgrade (ML Enrichment)
        try:
            from src.inference import InferenceEngine
            # Provide both paths explicitly if needed, but the engine now searches fallback /tmp/models
            engine = InferenceEngine(DB_PATH, model_dir="/tmp/models")
            engine.enrich_gold_layer()
        except Exception as e:
            logger.warning(f"ML Enrichment failed: {e}. Falling back to heuristic baseline.")
