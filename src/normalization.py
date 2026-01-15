"""
Normalization from staging → processed compensation tables.

Links staging Spotrac tables to internal dimensions and facts:
- Teams: normalize names → team codes used in `dim_players`/contracts
- Players: simple name-based linkage to `dim_players`
"""

from pathlib import Path
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STAGING_DIR = Path("data/staging")
PROCESSED_DIR = Path("data/processed/compensation")
PARQUET_DIR = PROCESSED_DIR / "parquet"

TEAM_NAME_TO_CODE = {
    'Arizona Cardinals':'ARI','Atlanta Falcons':'ATL','Baltimore Ravens':'BAL','Buffalo Bills':'BUF',
    'Carolina Panthers':'CAR','Chicago Bears':'CHI','Cincinnati Bengals':'CIN','Cleveland Browns':'CLE',
    'Dallas Cowboys':'DAL','Denver Broncos':'DEN','Detroit Lions':'DET','Green Bay Packers':'GB',
    'Houston Texans':'HOU','Indianapolis Colts':'IND','Jacksonville Jaguars':'JAX','Kansas City Chiefs':'KC',
    'Los Angeles Chargers':'LAC','Los Angeles Rams':'LAR','Las Vegas Raiders':'LV','Miami Dolphins':'MIA',
    'Minnesota Vikings':'MIN','New England Patriots':'NE','New Orleans Saints':'NO','New York Giants':'NYG',
    'New York Jets':'NYJ','Philadelphia Eagles':'PHI','Pittsburgh Steelers':'PIT','San Francisco 49ers':'SF',
    'Seattle Seahawks':'SEA','Tampa Bay Buccaneers':'TB','Tennessee Titans':'TEN','Washington Commanders':'WAS'
}


def _write_parquet(df: pd.DataFrame, table: str, year: int) -> None:
    """Write a Parquet sidecar partitioned by year; skip quietly if engine unavailable."""
    partition_dir = PARQUET_DIR / table / f"year={year}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(partition_dir / "part-000.parquet", index=False)
    except Exception as exc:  # tolerate missing pyarrow/fastparquet
        logger.warning("Parquet write skipped for %s %s: %s", table, year, exc)


def _map_team_name(name: str) -> str:
    return TEAM_NAME_TO_CODE.get(name, name)


def normalize_team_cap(year: int) -> Path:
    src = STAGING_DIR / f"stg_spotrac_team_cap_{year}.csv"
    if not src.exists():
        logger.warning("Staging team cap missing: %s", src)
        return src
    df = pd.read_csv(src)
    df['team'] = df['team_name'].apply(_map_team_name)
    out = PROCESSED_DIR / f"stg_team_cap_{year}.csv"
    df.to_csv(out, index=False)
    _write_parquet(df, "stg_team_cap", year)
    logger.info("Normalized team cap → %s (%d rows)", out, len(df))
    return out


def normalize_player_rankings(year: int) -> Path:
    src = STAGING_DIR / f"stg_spotrac_player_rankings_{year}.csv"
    players = pd.read_csv(PROCESSED_DIR / 'dim_players.csv') if (PROCESSED_DIR / 'dim_players.csv').exists() else pd.DataFrame()
    if not src.exists():
        logger.warning("Staging player rankings missing: %s", src)
        return src
    df = pd.read_csv(src)
    if not players.empty:
        # Simple name join (case-insensitive). Improve later with fuzzy + team + year.
        df['player_key'] = df['player_name'].str.strip().str.lower()
        players['player_key'] = players['player_name'].str.strip().str.lower()
        df = df.merge(players[['player_id','player_key']], on='player_key', how='left')
    out = PROCESSED_DIR / f"stg_player_rankings_{year}.csv"
    df.to_csv(out, index=False)
    _write_parquet(df, "stg_player_rankings", year)
    logger.info("Normalized player rankings → %s (%d rows)", out, len(df))
    return out


def normalize_dead_money(year: int) -> Path:
    src = STAGING_DIR / f"stg_spotrac_dead_money_{year}.csv"
    players = pd.read_csv(PROCESSED_DIR / 'dim_players.csv') if (PROCESSED_DIR / 'dim_players.csv').exists() else pd.DataFrame()
    if not src.exists():
        logger.warning("Staging dead money missing: %s", src)
        return src
    df = pd.read_csv(src)
    if not players.empty:
        df['player_key'] = df['player_name'].str.strip().str.lower()
        players['player_key'] = players['player_name'].str.strip().str.lower()
        df = df.merge(players[['player_id','player_key']], on='player_key', how='left')
    out = PROCESSED_DIR / f"stg_dead_money_{year}.csv"
    df.to_csv(out, index=False)
    _write_parquet(df, "stg_dead_money", year)
    logger.info("Normalized dead money → %s (%d rows)", out, len(df))
    return out

def normalize_year_data(year: int) -> None:
    """Normalize all staging data for a given year"""
    logger.info(f"Normalizing data for {year}")
    
    normalize_team_cap(year)
    normalize_player_rankings(year)
    normalize_dead_money(year)
    
    logger.info(f"✓ Normalization complete for {year}")


def main():
    """CLI interface"""
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description='Normalize staging data to processed layer')
    parser.add_argument('--year', type=int, required=True, help='Year to process')
    
    args = parser.parse_args()
    
    try:
        normalize_year_data(args.year)
    except Exception as e:
        logger.error(f"✗ Normalization failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()