
import pandas as pd
import numpy as np
from pathlib import Path
import glob
import logging
from src.config import DATA_RAW_DIR, DATA_PROCESSED_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
YEARS = range(2015, 2027) # 2015-2026 (includes future)
OUTPUT_DIR = DATA_PROCESSED_DIR
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "nfl_master_table.csv"

# Cap History (Approx with future projections)
CAP_HISTORY = {
    2015: 143.28, 2016: 155.27, 2017: 167.00, 2018: 177.20, 2019: 188.20,
    2020: 198.20, 2021: 182.50, 2022: 208.20, 2023: 224.80, 2024: 255.40,
    2025: 273.30, 2026: 290.00 # Projections
}

def clean_name(name):
    if pd.isna(name): return ""
    name = str(name).replace(" Jr.", "").replace(" Sr.", "").replace(" III", "").replace(" II", "")
    name_clean = name.strip().lower()
    
    # Fix: Spotrac Contracts often have "Lastname Firstname Lastname" (e.g. "Allen Josh Allen")
    # Heuristic: If first word equals last word, remove first word
    parts = name_clean.split()
    if len(parts) >= 3 and parts[0] == parts[-1]:
        return " ".join(parts[1:])
        
    return name_clean

def load_spotrac():
    logger.info("Loading Spotrac Financials...")
    dfs = []
    for year in YEARS:
        pattern = str(DATA_RAW_DIR / f"spotrac_player_rankings_{year}_*.csv")
        files = glob.glob(pattern)
        if files:
            latest = sorted(files)[-1]
            try:
                df = pd.read_csv(latest)
                df['year'] = year
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read {latest}: {e}")
        else:
            # Silence warning for future years if not yet scraped
            if year <= 2024:
                logger.debug(f"No Spotrac data for {year}")

    if not dfs:
        return pd.DataFrame()
    
    fin = pd.concat(dfs, ignore_index=True)

    # Normalize Columns (Required for join)
    fin['clean_name'] = fin['player_name'].apply(clean_name)

    # ---------------------------------------------------------
    # LOAD CONTRACTS (Canonical Age Source)
    # ---------------------------------------------------------
    logger.info("Loading Spotrac Contracts (for Age)...")
    contract_dfs = []
    for year in YEARS:
        pattern = str(DATA_RAW_DIR / f"spotrac_player_contracts_{year}_*.csv")
        files = glob.glob(pattern)
        if files:
            latest = sorted(files)[-1]
            try:
                cdf = pd.read_csv(latest)
                cdf['year'] = year
                contract_dfs.append(cdf)
            except Exception as e:
                logger.warning(f"Failed to read contracts {latest}: {e}")

    if contract_dfs:
        contracts = pd.concat(contract_dfs, ignore_index=True)
        # Normalize name for join
        contracts['clean_name'] = contracts['player_name'].apply(clean_name)
        
        # Prepare Age Lookup: clean_name + year -> age
        # Prepare Age Lookup: clean_name + year -> age
        # Deduplicate: take max age if duplicates (safety)
        age_lookup = contracts.dropna(subset=['age']).groupby(['clean_name', 'year'])['age'].max().reset_index()
        
        # Merge Age into Ranking Data
        rows_before = len(fin)
        fin = pd.merge(fin, age_lookup, on=['clean_name', 'year'], how='left', suffixes=('', '_contract'))
        
        # Override corrupted/missing rankings age with contract age
        if 'age_contract' in fin.columns:
            fin['age'] = fin['age_contract'].fillna(fin.get('age', np.nan))
    # ---------------------------------------------------------

    
    # Normalize Columns
    fin['clean_name'] = fin['player_name'].apply(clean_name)
    
    if 'cap_hit_millions' in fin.columns:
         fin['cap_hit_m'] = fin['cap_hit_millions']
    elif 'total_contract_value_millions' in fin.columns:
         fin['cap_hit_m'] = fin['total_contract_value_millions']
    
    # Fallback to parsing 'value' string if needed
    if 'cap_hit_m' not in fin.columns and 'value' in fin.columns:
        fin['cap_hit_m'] = fin['value'].replace('[\$,]', '', regex=True).astype(float) / 1_000_000
        
    fin['cap_hit_m'] = pd.to_numeric(fin['cap_hit_m'], errors='coerce').fillna(0)
    
    # Backfill Age from 2024/2025 data
    if 'age' in fin.columns:
        fin['age'] = pd.to_numeric(fin['age'], errors='coerce')
        
        # Create Anchor Map (Prefer 2024, then 2025)
        age_map = {}
        # Iterate through years descending to define anchor (2024 overrides 2025 if both exist? No, we want latest valid)
        # Actually 2024 is best anchor.
        # Get subset of players with valid age
        known_ages = fin[fin['age'].notna()].copy()
        for _, row in known_ages.iterrows():
            # Store Birth Year implies Age
            # Birth Year = Year - Age
            # This is constant.
            birth_year = row['year'] - row['age']
            age_map[row['clean_name']] = birth_year
            
        # Apply Backfill
        def fill_age_from_birth_year(row):
            if pd.notna(row['age']): return row['age']
            if row['clean_name'] in age_map:
                return row['year'] - age_map[row['clean_name']]
            return np.nan
            
        fin['age'] = fin.apply(fill_age_from_birth_year, axis=1)

    else:
        fin['age'] = np.nan
    
    # Add Cap Context
    fin['salary_cap'] = fin['year'].map(CAP_HISTORY)
    fin['cap_pct'] = (fin['cap_hit_m'] / fin['salary_cap']) * 100
    
    # Add Age if present
    if 'age' in fin.columns:
        fin['age'] = pd.to_numeric(fin['age'], errors='coerce')
    else:
        fin['age'] = np.nan

    return fin[['year', 'clean_name', 'player_name', 'team', 'position', 'age', 'cap_hit_m', 'cap_pct']]

def load_pfr():
    logger.info("Loading PFR Performance...")
    dfs = []
    # Performance is currently only valid up to 2024 (current playback)
    # 2025/2026 performance doesn't exist yet
    
    perf_years = range(2015, 2025)
    
    for year in perf_years:
        log_file = DATA_RAW_DIR / "pfr" / f"game_logs_{year}.csv"
        if Path(log_file).exists():
            try:
                logs = pd.read_csv(log_file)
                # Ensure clean headers (sometimes scraping leaves artifact)
                logs.columns = [c if 'Player' not in c else 'Player' for c in logs.columns]
                
                # Check for AV in logs (rarely there) or calc proxy
                # We reuse the logic from the analysis script
                if 'fantasy_points' not in logs.columns:
                    # Offensive Scoring
                    logs['fantasy_points_off'] = (
                        (pd.to_numeric(logs.get('Passing_Yds',0), errors='coerce').fillna(0)*0.04) + 
                        (pd.to_numeric(logs.get('Passing_TD',0), errors='coerce').fillna(0)*4) + 
                        (pd.to_numeric(logs.get('Rushing_Yds',0), errors='coerce').fillna(0)*0.1) + 
                        (pd.to_numeric(logs.get('Rushing_TD',0), errors='coerce').fillna(0)*6) +
                        (pd.to_numeric(logs.get('Receiving_Yds',0), errors='coerce').fillna(0)*0.1) + 
                        (pd.to_numeric(logs.get('Receiving_TD',0), errors='coerce').fillna(0)*6) -
                        (pd.to_numeric(logs.get('Passing_Int',0), errors='coerce').fillna(0)*2)
                    )

                    # Defensive Scoring (IDP) - Robust Column Lookup
                    def get_col(df, candidates):
                        for c in candidates:
                            # Check exact match or partial match for flattened columns
                            if c in df.columns: return pd.to_numeric(df[c], errors='coerce').fillna(0)
                        return 0.0

                    logs['fantasy_points_def'] = (
                        (get_col(logs, ['Def Interceptions_Int', 'Int']) * 6) +
                        (get_col(logs, ['Tackles_Solo', 'Solo']) * 1) +
                        (get_col(logs, ['Tackles_Ast', 'Ast']) * 0.5) +
                        (get_col(logs, ['Fumbles_FF', 'FF']) * 3) + 
                        (get_col(logs, ['Unnamed: 7_level_0_Sk', 'Sk', 'Sacks']) * 4) +
                        (get_col(logs, ['Def Interceptions_PD', 'PD']) * 1)
                    )
                    
                    logs['fantasy_points'] = logs['fantasy_points_off'] + logs['fantasy_points_def']
                
                # Aggregate to Season Level
                agg = logs.groupby(['Player'])['fantasy_points'].sum().reset_index()
                agg['year'] = year
                agg['AV_Proxy'] = agg['fantasy_points'] / 25
                agg['clean_name'] = agg['Player'].apply(clean_name)
                
                dfs.append(agg[['year', 'clean_name', 'AV_Proxy', 'fantasy_points']])
                
            except Exception as e:
                logger.error(f"Error reading PFR logs for {year}: {e}")
                
    if not dfs:
        return pd.DataFrame()
        
    return pd.concat(dfs, ignore_index=True)

def main():
    logger.info("Starting ETL Transform...")
    
    fin = load_spotrac()
    perf = load_pfr()
    
    logger.info(f"Financial Records: {len(fin)}")
    logger.info(f"Performance Records: {len(perf)}")
    
    if fin.empty:
        logger.error("No financial data found. Aborting.")
        return

    # Merge
    # Left join to keep all contracts (even future ones without performance)
    master = pd.merge(fin, perf, on=['clean_name', 'year'], how='left')
    
    # Calculate Efficiency (AV / Cap %)
    # Handle NaN for future years or missing performance
    master['efficiency'] = master['AV_Proxy'].fillna(0) / master['cap_pct'].clip(lower=0.1)
    
    # Flag Future Years
    master['is_future'] = master['year'] > 2024
    
    logger.info(f"Saving Master Dataset ({len(master)} rows) to {OUTPUT_FILE}...")
    master.to_csv(OUTPUT_FILE, index=False)
    logger.info("ETL Complete.")

if __name__ == "__main__":
    main()
