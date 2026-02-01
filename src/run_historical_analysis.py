
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import glob
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
YEARS = range(2015, 2025)
OUTPUT_DIR = Path("data/processed/historical_viz")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Cap History (Approx)
CAP_HISTORY = {
    2015: 143.28, 2016: 155.27, 2017: 167.00, 2018: 177.20, 2019: 188.20,
    2020: 198.20, 2021: 182.50, 2022: 208.20, 2023: 224.80, 2024: 255.40
}

def clean_name(name):
    if pd.isna(name): return ""
    name = str(name).replace(" Jr.", "").replace(" Sr.", "").replace(" III", "").replace(" II", "")
    return name.strip().lower()

def load_data():
    logger.info("Loading Financials...")
    dfs_fin = []
    for year in YEARS:
        pattern = f"data/raw/spotrac_player_rankings_{year}_*.csv"
        files = glob.glob(pattern)
        if files:
            latest = sorted(files)[-1]
            df = pd.read_csv(latest)
            df['year'] = year
            dfs_fin.append(df)
    
    financials = pd.concat(dfs_fin, ignore_index=True) if dfs_fin else pd.DataFrame()
    
    logger.info("Loading Performance (PFR Log Aggregation)...")
    dfs_perf = []
    
    # We prioritize Logs if available, else Rosters
    for year in YEARS:
        log_file = f"data/raw/pfr/game_logs_{year}.csv"
        
        if Path(log_file).exists():
            # Using logs: Aggregating fantasy points as AV proxy or utilizing AV if present in summary
            # For simplicity in this script, we'll calculate a "Performance Score" from logs
            # if AV isn't directly available.
            # In the 2025 scrape, logs have 'AV' only in summaries.
            # Let's assume we calc 'fantasy_points' sum.
            
            try:
                logs = pd.read_csv(log_file)
                # Clean columns
                logs.columns = [c if 'Player' not in c else 'Player' for c in logs.columns]
                
                # Check if we have stats columns
                stats = ['Passing_Yds', 'Passing_TD', 'Rushing_Yds', 'Rushing_TD', 'Receiving_Yds', 'Receiving_TD']
                for c in stats:
                    if c in logs.columns:
                        logs[c] = pd.to_numeric(logs[c], errors='coerce').fillna(0)
                
                # Simple Fantasy Point Calc for approx value
                # (Same log as monthly efficiency)
                if 'fantasy_points' not in logs.columns:
                    logs['fantasy_points'] = (
                        (logs.get('Passing_Yds',0)*0.04) + (logs.get('Passing_TD',0)*4) + 
                        (logs.get('Rushing_Yds',0)*0.1) + (logs.get('Rushing_TD',0)*6) +
                        (logs.get('Receiving_Yds',0)*0.1) + (logs.get('Receiving_TD',0)*6)
                    )
                
                agg = logs.groupby(['Player'])['fantasy_points'].sum().reset_index()
                agg['year'] = year
                agg['AV_Proxy'] = agg['fantasy_points'] / 25 # Rough scale to AV (300 pts ~ 12 AV)
                dfs_perf.append(agg)
            except Exception as e:
                logger.warning(f"Error processing logs for {year}: {e}")
                
    performance = pd.concat(dfs_perf, ignore_index=True) if dfs_perf else pd.DataFrame()
    return financials, performance

def generate_plots(merged):
    logger.info("Generating plots...")
    sns.set_style("whitegrid")
    
    # 1. Positional Trends
    major_positions = ['QB', 'WR', 'RB', 'CB', 'DE', 'TE', 'OL']
    subset = merged[merged['position'].isin(major_positions)]
    
    plt.figure(figsize=(14, 8))
    sns.lineplot(data=subset, x='year', y='efficiency', hue='position', marker='o', ci='sd')
    plt.title("Positional Efficiency Trends (2015-2024)\n(Efficiency = Perf / Cap%)")
    plt.savefig(OUTPUT_DIR / "positional_trends_10yr.png")
    plt.close()
    
    # 2. Career Arcs
    stars = ['tom brady', 'aaron rodgers', 'patrick mahomes', 'aaron donald', 'todd gurley', 'julio jones']
    subset = merged[merged['clean_name'].isin(stars)].sort_values(['clean_name', 'year'])
    
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=subset, x='year', y='efficiency', hue='clean_name', marker='o', linewidth=2.5)
    plt.title("Career Efficiency Arcs")
    plt.axhline(y=1.0, color='gray', linestyle='--', label='Replacement')
    plt.legend()
    plt.savefig(OUTPUT_DIR / "career_arcs.png")
    plt.close()
    
    # 3. Replacement Trap (2023 focus for cleanliness)
    recent = merged[(merged['year'] == 2023) & (merged['AV_Proxy'] < 4)].sort_values('cap_pct', ascending=False).head(20)
    
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=recent, x='cap_pct', y='AV_Proxy', hue='position', s=100)
    for i, row in recent.iterrows():
        plt.text(row['cap_pct'], row['AV_Proxy'], row['player_name'], fontsize=8)
    plt.title("The Replacement Trap (2023): High Cost, Low Output")
    plt.xlabel("Cap Hit %")
    plt.ylabel("Performance (Est. AV)")
    plt.savefig(OUTPUT_DIR / "replacement_trap.png")
    plt.close()

def main():
    fin, perf = load_data()
    
    fin['clean_name'] = fin['player_name'].apply(clean_name)
    perf['clean_name'] = perf['Player'].apply(clean_name)
    
    # Normalize Fin
    if 'cap_hit_millions' in fin.columns:
         fin['cap_hit_m'] = fin['cap_hit_millions']
    elif 'total_contract_value_millions' in fin.columns:
         fin['cap_hit_m'] = fin['total_contract_value_millions']
    elif 'value' in fin.columns:
         fin['cap_hit_m'] = fin['value'].replace('[\$,]', '', regex=True).astype(float) / 1_000_000
    else:
         # Fallback if we really can't find it, though we should check headers
         fin['cap_hit_m'] = 0
         
    fin['salary_cap'] = fin['year'].map(CAP_HISTORY)
    fin['cap_pct'] = (fin['cap_hit_m'] / fin['salary_cap']) * 100
    
    # Merge
    merged = pd.merge(fin, perf, on=['clean_name', 'year'], how='inner')
    merged['efficiency'] = merged['AV_Proxy'] / merged['cap_pct'].clip(lower=0.1)
    
    logger.info(f"Merged {len(merged)} player-seasons.")
    generate_plots(merged)
    logger.info("Done.")

if __name__ == "__main__":
    main()
