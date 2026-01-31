"""
Functions for calculating player value metrics.
"""

import pandas as pd
import numpy as np

def calculate_value_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Efficiency and Value Deltas for players.
    
    Expected columns:
    - total_contract_value_millions
    - contract_length_years
    - performance_av (or AV)
    """
    df = df.copy()
    
    # Calculate APY (Average Per Year)
    # Using total_contract_value / contract_length
    if 'total_contract_value_millions' in df.columns and 'contract_length_years' in df.columns:
        df['salary_apy'] = df['total_contract_value_millions'] / df['contract_length_years']
    
    # Simple efficiency metric: AV per Million APY
    if 'performance_av' in df.columns and 'salary_apy' in df.columns:
        # Avoid division by zero
        df['efficiency_av_per_million'] = df['performance_av'] / df['salary_apy'].replace(0, np.nan)
    elif 'AV' in df.columns and 'salary_apy' in df.columns:
        df['efficiency_av_per_million'] = df['AV'] / df['salary_apy'].replace(0, np.nan)
        
    return df

def identify_outliers(df: pd.DataFrame, metric: str = 'efficiency_av_per_million', top_n: int = 10):
    """Identify top and bottom N players based on a metric."""
    top_players = df.nlargest(top_n, metric)
    bottom_players = df.nsmallest(top_n, metric)
    return top_players, bottom_players
