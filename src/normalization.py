"""
Normalization from staging → processed compensation tables.

Links staging Spotrac tables to internal dimensions and facts:
- Teams: normalize names → team codes used in `dim_players`/contracts
- Players: simple name-based linkage to `dim_players`
- Rosters: join PFR roster data (age, performance) to player records
- Contracts: normalize contract financial data for prediction features
"""

from pathlib import Path
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STAGING_DIR = Path("data/staging")
PROCESSED_DIR = Path("data/processed/compensation")
ROSTERS_DIR = Path("data/processed/rosters")
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
    # Handle both 'team_name' and 'team' columns
    if 'team_name' in df.columns:
        df['team'] = df['team_name'].apply(_map_team_name)
    elif 'team' in df.columns:
        # If team column is already team code, keep it; if full name, normalize
        df['team'] = df['team'].apply(lambda x: _map_team_name(x) if len(x) > 2 else x)
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

def normalize_player_contracts(year: int) -> Path:
    """
    Normalize player contract data from Spotrac team contracts pages.
    Joins with PFR roster data (age, performance metrics) for enrichment.
    
    Returns path to normalized contracts CSV.
    """
    src = STAGING_DIR / f"stg_spotrac_player_contracts_{year}.csv"
    if not src.exists():
        logger.warning("Staging contracts missing: %s", src)
        return src
    
    df = pd.read_csv(src)
    
    # Try loading from processed player rankings first (from normalization)
    rankings_src = PROCESSED_DIR / f"stg_player_rankings_{year}.csv"
    if rankings_src.exists():
        try:
            rankings = pd.read_csv(rankings_src)
            df['player_key'] = df['player_name'].str.strip().str.lower()
            rankings['player_key'] = rankings['player_name'].str.strip().str.lower()
            
            # Join on player_key to enrich with age and AV
            columns_to_join = [c for c in ['player_key', 'age_at_signing', 'performance_av', 'games_played_prior_year', 'years_experience'] if c in rankings.columns]
            if columns_to_join:
                df = df.merge(rankings[columns_to_join], on='player_key', how='left', suffixes=('', '_roster'))
                logger.info(f"  ✓ Enriched {df[['age_at_signing']].notna().sum()} players with roster data")
        except Exception as e:
            logger.warning("Failed to enrich contracts with rankings data: %s", e)
    
    # Normalize team codes
    df['team'] = df['team'].apply(_map_team_name)
    
    out = PROCESSED_DIR / f"stg_player_contracts_{year}.csv"
    df.to_csv(out, index=False)
    _write_parquet(df, "stg_player_contracts", year)
    logger.info("Normalized contracts → %s (%d rows)", out, len(df))
    return out

def normalize_dead_money_with_features(year: int) -> Path:
    """
    Normalize dead money data and join with contract/roster features for prediction.
    
    Returns enriched dead money CSV with player features.
    """
    dead_src = PROCESSED_DIR / f"stg_dead_money_{year}.csv"
    contract_src = PROCESSED_DIR / f"stg_player_contracts_{year}.csv"
    
    if not dead_src.exists():
        logger.warning("Dead money file missing: %s", dead_src)
        return dead_src
    
    df = pd.read_csv(dead_src)
    
    # Join with contract details (if available)
    if contract_src.exists():
        contracts = pd.read_csv(contract_src)
        # Join on player name, team, year
        feature_cols = ['player_name', 'team', 'year']
        # Add contract financial columns if they exist
        feature_cols.extend([c for c in ['guaranteed_money_millions', 'signing_bonus_millions', 
                                         'contract_length_years', 'years_remaining', 
                                         'age_at_signing', 'performance_av', 'games_played_prior_year', 'years_experience'] 
                            if c in contracts.columns])
        
        df = df.merge(
            contracts[feature_cols],
            on=['player_name', 'team', 'year'],
            how='left'
        )
        logger.info(f"  ✓ Joined {df[contracts.columns[0]].notna().sum()} contracts to dead money")
    
    out = PROCESSED_DIR / f"dead_money_features_{year}.csv"
    df.to_csv(out, index=False)
    _write_parquet(df, "dead_money_features", year)
    logger.info("Normalized dead money features → %s (%d rows)", out, len(df))
    return out

def normalize_year_data(year: int) -> None:
    """Normalize all staging data for a given year"""
    logger.info(f"Normalizing data for {year}")
    
    normalize_team_cap(year)
    normalize_player_rankings(year)
    normalize_dead_money(year)
    normalize_player_contracts(year)
    normalize_dead_money_with_features(year)
    
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