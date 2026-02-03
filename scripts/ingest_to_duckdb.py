
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



def find_targeted_files(pattern_name: str, year: int, week: int = None):
    """
    Finds the specific file for a given pattern based on parameters.
    First Principle: Explicit Determinism.
    Input: data/raw/{year}/week_{week}_{ts}/
    """
    if year is None:
        raise ValueError("Year parameter is required for explicit ingestion.")
        
    year_dir = RAW_DIR / str(year)
    if not year_dir.exists():
        logger.warning(f"Year directory {year_dir} does not exist.")
        return []

    # If week is specified, look for week_{week}_*
    # If week is NOT specified, we fall back to "latest available for that year" 
    # BUT properly we should demand explicit inputs. 
    # For now, to support the user's "single pipeline" flow where week might be implicit if running locally without it,
    # we'll look for the latest run of that year IF week is missing.
    # But strictly, the DAG passes both.
    
    target_pattern = f"week_{week}_*" if week else "*"
    subdirs = sorted([d for d in year_dir.glob(target_pattern) if d.is_dir()])
    
    if not subdirs:
        logger.warning(f"No data found for Year={year}, Week={week}")
        # Check for legacy flat files if week is None
        if week is None:
             legacy = list(RAW_DIR.glob(f"{pattern_name}_{year}_*.csv"))
             return [legacy[-1]] if legacy else []
        return []

    # If multiple timestamps for same week, take latest (Repair/Resync logic)
    target_dir = subdirs[-1]
    logger.info(f"Targeting source data: {target_dir}")
    
    # Verify file exists in that dir
    files = list(target_dir.glob(f"{pattern_name}*.csv"))
    return [files[0]] if files else []

def ingest_spotrac(year: int, week: int):
    con = duckdb.connect(DB_PATH)
    logger.info(f"Ingesting Spotrac (Year={year}, Week={week}) - Upsert Mode")
    
    files = find_targeted_files("spotrac_player_contracts", year, week)
    if not files:
        logger.warning("Skipping Spotrac ingest: No file found.")
        con.close()
        return

    # Ingest Temporary Table
    df = pd.read_csv(files[0])
    df['player_name'] = df['player_name'].apply(clean_doubled_name)
    
    # UPSERT Logic: Contracts
    con.execute("CREATE TABLE IF NOT EXISTS silver_spotrac_contracts AS SELECT * FROM df LIMIT 0")
    con.execute(f"DELETE FROM silver_spotrac_contracts WHERE year = {year}")
    con.execute("INSERT INTO silver_spotrac_contracts BY NAME SELECT * FROM df")
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
             con.execute("INSERT INTO silver_spotrac_salaries SELECT * FROM df_sal")
        except duckdb.ConstraintException:
             # Handle schema skew if strict
             logger.warning("Schema mismatch in salaries, falling back to loose insert")
             # Flatten Columns logic if needed, but for now assume schema compatibility or use by-name
             con.execute("INSERT INTO silver_spotrac_salaries BY NAME SELECT * FROM df_sal")
             
        logger.info(f"✓ Upserted rows into silver_spotrac_salaries for {year}")

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
    
    files = find_targeted_files("game_logs", year, week)
    if not files:
        # Retry with prefix prefix
        files = find_targeted_files("pfr_game_logs", year, week)
        
    if not files:
        logger.warning("Skipping PFR ingest: No file found.")
        con.close()
        return

    df = pd.read_csv(files[0])
    pfr_col = 'Unnamed: 0_level_0_Player' if 'Unnamed: 0_level_0_Player' in df.columns else 'Player'
    if pfr_col in df.columns:
        df = df.rename(columns={pfr_col: 'player_name'})
        df['player_name'] = df['player_name'].str.replace('*', '', regex=False).str.replace('+', '', regex=False).str.strip()
    
    # UPSERT Logic
    con.execute("CREATE TABLE IF NOT EXISTS silver_pfr_game_logs AS SELECT * FROM df LIMIT 0")
    con.execute(f"DELETE FROM silver_pfr_game_logs WHERE year = {year}")
    con.execute("INSERT INTO silver_pfr_game_logs BY NAME SELECT * FROM df")
    logger.info(f"✓ Upserted rows into silver_pfr_game_logs for {year}")
    
    con.close()

def ingest_financials():
    # Financials are static for now, no partition logic needed yet
    con = duckdb.connect(DB_PATH)
    fin_path = RAW_DIR / "finance/team_valuations_2024.csv"
    load_team_financials(con, fin_path)
    merch_path = RAW_DIR / "merch/nflpa_player_sales_2024.csv"
    load_player_merch(con, merch_path)
    con.close()

def ingest_penalties(year: int):
    con = duckdb.connect(DB_PATH)
    logger.info(f"Ingesting Penalties (Year={year})")
    
    # Penalties are usually by year, no week partition in our raw storage for this source yet
    files = list(RAW_DIR.glob(f"penalties/improved_penalties_{year}_*.csv"))
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
    con.execute(f"DELETE FROM silver_penalties WHERE year = {year}")
    con.execute("INSERT INTO silver_penalties BY NAME SELECT * FROM df")
    logger.info(f"✓ Upserted rows into silver_penalties for {year}")
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
            s.team, s.year, s.position,
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
            COALESCE(p.total_pass_yds, 0) as total_pass_yds,
            COALESCE(p.total_rush_yds, 0) as total_rush_yds,
            COALESCE(p.total_rec_yds, 0) as total_rec_yds,
            COALESCE(p.total_tds, 0) as total_tds,
            af.corrected_age as age_final,
            af.college, af.draft_round, af.draft_pick, af.experience_years,
            
            -- Penalties
            COALESCE(pen.total_penalty_count, 0) as total_penalty_count,
            COALESCE(pen.total_penalty_yards, 0) as total_penalty_yards,
            
            -- Financial Lift
            COALESCE(pm.popularity_score, 0) as popularity_score,
            COALESCE(pm.Rank, 999) as merch_rank,
            tc.Revenue_M as team_revenue,
            tc.OperatingIncome_M as team_op_income
        FROM dedup_contracts s
        LEFT JOIN pfr_agg p ON LOWER(TRIM(CAST(s.player_name AS VARCHAR))) = LOWER(TRIM(CAST(p.player_name AS VARCHAR))) AND s.year = p.year
        -- Fuzzy join for penalties (Short name L.Tunsil starts with first letter + contains last name)
        LEFT JOIN penalties_agg pen ON s.year = pen.year AND s.team = pen.team
            AND (LOWER(s.player_name) LIKE LOWER(LEFT(pen.player_name_short, 1)) || '%' AND LOWER(s.player_name) LIKE '%' || LOWER(SUBSTRING(pen.player_name_short, 3)))
        LEFT JOIN age_fix af ON s.player_name = af.player_name AND s.year = af.year
        LEFT JOIN (
            SELECT Player, MIN(Rank) as Rank, MAX(51 - Rank) as popularity_score FROM silver_player_merch GROUP BY 1
        ) pm ON LOWER(TRIM(CAST(s.player_name AS VARCHAR))) = LOWER(TRIM(CAST(pm.Player AS VARCHAR)))
        LEFT JOIN silver_team_finance tc ON LOWER(TRIM(CAST(s.team AS VARCHAR))) = LOWER(TRIM(CAST(tc.Team AS VARCHAR))) AND tc.Year = 2023
    )
    SELECT 
        f.player_name, f.team, f.position, f.year, f.cap_hit_millions,
        GREATEST(COALESCE(sdc.salaries_dead_cap_millions, 0), f.dead_cap_millions, COALESCE(f.signing_bonus_millions, 0) * 2.0, COALESCE(f.guaranteed_money_millions, 0)) as potential_dead_cap_millions,
        f.age_final as age,
        f.college, f.draft_round, f.draft_pick, f.experience_years,
        f.games_played, f.total_pass_yds, f.total_rush_yds, f.total_rec_yds, f.total_tds,
        f.total_penalty_count, f.total_penalty_yards,
        
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
        ( ( (COALESCE(f.total_tds,0) * 6 + (COALESCE(f.total_pass_yds,0) + COALESCE(f.total_rush_yds,0) + COALESCE(f.total_rec_yds,0)) / 20.0) / NULLIF(f.cap_hit_millions, 0) ) + ( ( (f.popularity_score * 0.2) + (CASE WHEN f.merch_rank <= 10 THEN 5.0 WHEN f.merch_rank <= 50 THEN 2.0 ELSE 0 END) ) / NULLIF(f.cap_hit_millions, 0) ) ) as combined_roi_score
        
    FROM fact_long_fallback f
    LEFT JOIN age_risk_theshold ar ON f.player_name = ar.player_name AND f.year = ar.year
    LEFT JOIN salary_dead_cap sdc ON LOWER(TRIM(CAST(f.player_name AS VARCHAR))) = LOWER(TRIM(CAST(sdc.player_name AS VARCHAR))) AND f.year = sdc.year AND LOWER(TRIM(CAST(f.team AS VARCHAR))) = LOWER(TRIM(CAST(sdc.team AS VARCHAR)))
    """)
    
    logger.info("✓ Gold Layer created: fact_player_efficiency (Penalty-Aware)")
    con.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Specific year to ingest (required)", required=True)
    parser.add_argument("--week", type=int, help="Specific week (optional)", default=None)
    args = parser.parse_args()

    ingest_spotrac(year=args.year, week=args.week)
    ingest_pfr(year=args.year, week=args.week)
    ingest_penalties(year=args.year)
    ingest_financials()
    create_gold_layer()
