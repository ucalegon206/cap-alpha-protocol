
import requests
import pandas as pd
from pathlib import Path
import time
import random
import logging
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Spotrac Team Mapping
TEAM_MAP = {
    "ARI": "ARI", "ATL": "ATL", "BAL": "BAL", "BUF": "BUF", "CAR": "CAR", 
    "CHI": "CHI", "CIN": "CIN", "CLE": "CLE", "DAL": "DAL", "DEN": "DEN",
    "DET": "DET", "GNB": "GB", "HOU": "HOU", "IND": "IND", "JAX": "JAX",
    "KAN": "KC", "LAC": "LAC", "LAR": "LAR", "LVR": "LV", "MIA": "MIA",
    "MIN": "MIN", "NWE": "NE", "NOR": "NO", "NYG": "NYG", "NYJ": "NYJ",
    "PHI": "PHI", "PIT": "PIT", "SFO": "SF", "SEA": "SEA", "TAM": "TB",
    "TEN": "TEN", "WAS": "WAS"
}

def scrape_pfr_gamelogs(year):
    # 1. Scrape Fantasy (Offense)
    url_off = f"https://www.pro-football-reference.com/years/{year}/fantasy.htm"
    logger.info(f"Scraping Offense: {url_off}...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
    }
    
    try:
        # --- OFFENSE ---
        res_off = requests.get(url_off, headers=headers)
        res_off.raise_for_status()
        dfs_off = pd.read_html(res_off.text, header=1)
        df_off = dfs_off[0]
        df_off = df_off[df_off['Player'] != 'Player']
        
        # Normalize Offense
        df_final = pd.DataFrame()
        
        # Robust Column Finding
        def get_col(df, candidates):
            for c in candidates:
                if c in df.columns: return df[c]
            return pd.Series([None]*len(df))

        df_final['player_name'] = get_col(df_off, ['Player', 'Player_Name'])
        df_final['team'] = get_col(df_off, ['Tm', 'Team'])
        df_final['position'] = get_col(df_off, ['FantPos', 'Pos'])
        df_final['year'] = year
        df_final['game_url'] = f"https://www.pro-football-reference.com/years/{year}/"
        
        # Numeric Stats (Offense)
        # Passing: Yds, TD
        # Rushing: Yds, TD
        # Receiving: Yds, TD
        # Since these might have duplicate names (Yds, Yds.1), we must be careful with read_html
        # The flattened header usually results in: Yds, Yds.1, Yds.2
        # Standard PFR Fantasy Table Order: Passing, Rushing, Receiving
        
        # We'll rely on iloc for stats because names are duplicated, BUT we verify index 0/1/2 first
        
        # Offense Table Columns check
        # We know Player/Tm/Pos are usually at start. 
        # Let's assume the previous iloc for STATS was correct (since A.J. Brown stats looked ok), 
        # but fixing Player/Team is priority.
        
        df_final['Passing_Yds'] = pd.to_numeric(df_off.iloc[:, 9], errors='coerce').fillna(0)
        df_final['Passing_TD'] = pd.to_numeric(df_off.iloc[:, 10], errors='coerce').fillna(0)
        df_final['Rushing_Yds'] = pd.to_numeric(df_off.iloc[:, 13], errors='coerce').fillna(0)
        df_final['Rushing_TD'] = pd.to_numeric(df_off.iloc[:, 15], errors='coerce').fillna(0)
        df_final['Receiving_Yds'] = pd.to_numeric(df_off.iloc[:, 18], errors='coerce').fillna(0)
        df_final['Receiving_TD'] = pd.to_numeric(df_off.iloc[:, 20], errors='coerce').fillna(0)
        
        # --- DEFENSE ---
        url_def = f"https://www.pro-football-reference.com/years/{year}/defense.htm"
        logger.info(f"Scraping Defense: {url_def}...")
        
        time.sleep(random.uniform(2, 4))
        
        res_def = requests.get(url_def, headers=headers)
        res_def.raise_for_status()
        dfs_def = pd.read_html(res_def.text, header=1)
        df_def = dfs_def[0]
        df_def = df_def[df_def['Player'] != 'Player']
        
        logger.info(f"Defense Columns: {df_def.columns.tolist()}")
        logger.info(f"Defense Head: {df_def.head().to_string()}")
        
        # Defense Columns:
        # Player, Tm, Age, Pos, G, GS, Int, Yds, TD, Lng, PD, FF, Fmb, FR, Yds, TD, Sk, Comb, Solo, Ast, TFL, QBHits
        # Note: 'Sk' is Sacks. 'Int' is Interceptions.
        
        df_d = pd.DataFrame()
        df_d['player_name'] = get_col(df_def, ['Player'])
        df_d['team'] = get_col(df_def, ['Tm', 'Team'])
        
        # Stats
        df_d['Sacks'] = pd.to_numeric(get_col(df_def, ['Sk', 'Sacks']), errors='coerce').fillna(0)
        df_d['Interceptions'] = pd.to_numeric(get_col(df_def, ['Int', 'Interceptions']), errors='coerce').fillna(0)
        
        # Drop duplicates in defense (agg if needed? usually one row per player per year)
        df_d = df_d.groupby(['player_name', 'team']).sum().reset_index()
        
        # Map Team Abbrs in BOTH dfs
        df_final['team'] = df_final['team'].map(TEAM_MAP).fillna(df_final['team'])
        df_d['team'] = df_d['team'].map(TEAM_MAP).fillna(df_d['team'])
        
        df_final['team'] = df_final['team'].astype(str)
        df_d['team'] = df_d['team'].astype(str)
        
        # Merge keys
        df_final['merge_key'] = df_final['player_name'].astype(str).str.replace('*', '', regex=False).str.replace('+', '', regex=False).str.strip()
        df_d['merge_key'] = df_d['player_name'].astype(str).str.replace('*', '', regex=False).str.replace('+', '', regex=False).str.strip()
        
        df_merged = pd.merge(df_final, df_d[['merge_key', 'team', 'Sacks', 'Interceptions']], 
                             on=['merge_key', 'team'], 
                             how='outer',
                             suffixes=('', '_def'))
                             
        # Coalesce
        # If player was only in defense, they might have NaN for offense stats
        numeric_cols = ['Passing_Yds', 'Passing_TD', 'Rushing_Yds', 'Rushing_TD', 'Receiving_Yds', 'Receiving_TD', 'Sacks', 'Interceptions']
        for col in numeric_cols:
            if col in df_merged.columns:
                df_merged[col] = df_merged[col].fillna(0)
        
        # Fill Metadata for defense-only players
        df_merged['player_name'] = df_merged['player_name'].fillna(df_merged['merge_key']) # simplify
        df_merged['year'] = df_merged['year'].fillna(year)
        df_merged['game_url'] = df_merged['game_url'].fillna(f"https://www.pro-football-reference.com/years/{year}/")
        
        # Final cleanup
        df_out = df_merged.drop(columns=['merge_key'])
        
        # FIX: Apply cleaning to the final player_name column (remove *, +) because Spotrac keys don't have them
        df_out['player_name'] = df_out['player_name'].astype(str).str.replace('*', '', regex=False).str.replace('+', '', regex=False).str.strip()
        
        # Filter out junk
        df_out = df_out.dropna(subset=['player_name'])
        
        # FIX: Deduplicate. If a player/team combo appears twice, keep the one with most games or just first.
        # This handles the case if outer join created ghosts or PFR has duplicates.
        df_out = df_out.drop_duplicates(subset=['player_name', 'team'], keep='first')
        
        # Save
        # Save
        output_dir = Path(f"data/raw/pfr/{year}")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"game_logs_{year}.csv"
        
        df_out.to_csv(output_path, index=False)
        logger.info(f"Saved {len(df_out)} rows (Merged Off+Def) to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to scrape PFR: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    scrape_pfr_gamelogs(parser.parse_args().year)
