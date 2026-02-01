import pytest
import pandas as pd
from pathlib import Path
from src.spotrac_scraper_v2 import SpotracParser, DataQualityError

@pytest.fixture
def parser():
    return SpotracParser()

@pytest.fixture
def sample_html():
    path = Path(__file__).parent / "fixtures" / "sample_contracts.html"
    return path.read_text()

def test_parse_table_basic(parser, sample_html):
    """Verify headers and rows are extracted correctly from fixture"""
    headers, rows = parser.parse_table(sample_html)
    
    assert "player" in headers
    # verify deduplication (our fixture has unique but we test logic)
    assert len(headers) == 5
    assert len(rows) == 3
    assert rows[0][0] == "Patrick Mahomes"

def test_parse_money_formats(parser):
    """Test various money formats found on Spotrac"""
    assert parser.parse_money("$450,000,000") == 450.0
    assert parser.parse_money("$255.4M") == 255.4
    assert parser.parse_money("$1.2B") == 1200.0
    assert parser.parse_money("$25K") == 0.025
    assert parser.parse_money("-") == 0.0
    assert parser.parse_money("") == 0.0
    assert parser.parse_money(None) == 0.0

def test_normalize_player_df_contracts(parser, sample_html):
    """Test full normalization pipeline for contract data"""
    headers, rows = parser.parse_table(sample_html)
    df_raw = pd.DataFrame(rows, columns=headers)
    df_raw['team'] = 'KC' # Add team which isn't in fixture but expected
    
    df_norm = parser.normalize_player_df(df_raw, 2024)
    
    assert 'player_name' in df_norm.columns
    assert 'total_contract_value_millions' in df_norm.columns
    assert 'guaranteed_money_millions' in df_norm.columns
    assert df_norm.loc[0, 'player_name'] == "Patrick Mahomes"
    assert df_norm.loc[0, 'total_contract_value_millions'] == 450.0

def test_normalize_schema_variations(parser):
    """Test handling of 'Name' vs 'Player' headers"""
    data = [['Mahomes', 'KC', '$450M'], ['Allen', 'BUF', '$258M']]
    
    # Version 1: "Player"
    df1 = pd.DataFrame(data, columns=['Player', 'Team', 'Value'])
    df1_norm = parser.normalize_player_df(df1, 2024)
    assert df1_norm.iloc[0]['player_name'] == 'Mahomes'
    
    # Version 2: "Name"
    df2 = pd.DataFrame(data, columns=['Name', 'Team', 'Value'])
    df2_norm = parser.normalize_player_df(df2, 2024)
    assert df2_norm.iloc[0]['player_name'] == 'Mahomes'

def test_validate_player_data_fails_on_empty(parser):
    """Validator should raise error for empty data"""
    df = pd.DataFrame()
    with pytest.raises(DataQualityError, match="Expected ≥50 players"):
        parser.validate_player_data(df, 2024)

def test_validate_player_data_passes_valid(parser):
    """Validator should pass for reasonably sized dataframe"""
    data = {
        'player_name': [f'Player {i}' for i in range(60)],
        'team': ['KC'] * 60,
        'year': [2024] * 60
    }
    df = pd.DataFrame(data)
    # Should not raise
    parser.validate_player_data(df, 2024, min_rows=50)
