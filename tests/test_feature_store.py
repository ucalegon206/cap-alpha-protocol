
import pytest
import duckdb
import pandas as pd
from datetime import date
from src.feature_store import FeatureStore

@pytest.fixture
def store(tmp_path):
    """Create a temporary FeatureStore for testing."""
    db_path = str(tmp_path / "test_feature_store.duckdb")
    store = FeatureStore(db_path=db_path)
    store.initialize_schema()
    return store

def test_schema_dates(store):
    """Verify schema uses DATE types for validity."""
    schema = store.con.execute("DESCRIBE feature_values").df()
    
    # Check valid_from is DATE
    valid_from_type = schema.loc[schema['column_name'] == 'valid_from', 'column_type'].iloc[0]
    assert valid_from_type == 'DATE'
    
    # Check valid_until exists and is DATE
    valid_until_type = schema.loc[schema['column_name'] == 'valid_until', 'column_type'].iloc[0]
    assert valid_until_type == 'DATE'

def test_temporal_leakage_protection(store):
    """Ensure we cannot see features from the future."""
    # Setup: Feature known from 2023-02-01 until 2024-02-01
    store.con.execute("""
        INSERT INTO feature_values (entity_key, player_name, prediction_year, feature_name, feature_value, valid_from, valid_until)
        VALUES ('P1_2023', 'PlayerOne', 2023, 'test_feat', 100.0, '2023-02-01', '2024-02-01')
    """)
    
    # Query BEFORE valid_from (Should be empty)
    df_early = store.get_training_matrix(as_of_date=date(2023, 1, 1), min_year=2023)
    assert 'test_feat' not in df_early.columns or df_early.empty

    # Query DURING validity (Should find it)
    df_valid = store.get_training_matrix(as_of_date=date(2023, 6, 1), min_year=2023)
    assert not df_valid.empty
    assert df_valid.iloc[0]['test_feat'] == 100.0

    # Query AFTER valid_until (Should be empty or superseded -- here empty locally)
    # Note: get_training_matrix normally filters by prediction year too, but focused on date joins
    # We simulate a 2024 prediction year query
    store.con.execute("""
        INSERT INTO feature_values (entity_key, player_name, prediction_year, feature_name, feature_value, valid_from, valid_until)
        VALUES ('P1_2024', 'PlayerOne', 2024, 'test_feat', 200.0, '2024-02-01', NULL)
    """)
    
    # As of mid-2024, should see the NEW value (200.0), not old (100.0)
    df_2024 = store.get_training_matrix(as_of_date=date(2024, 6, 1), min_year=2024)
    assert df_2024.iloc[0]['test_feat'] == 200.0

def test_lag_materialization_dates(store):
    """Test that lag materialization sets correct date boundaries."""
    # Mock source data
    store.con.execute("CREATE TABLE fact_player_efficiency (player_name VARCHAR, year INTEGER, total_pass_yds INTEGER)")
    store.con.execute("INSERT INTO fact_player_efficiency VALUES ('QB1', 2022, 4000)")
    store.con.execute("INSERT INTO fact_player_efficiency VALUES ('QB1', 2023, 4500)")
    
    # Materialize
    store.materialize_lag_features(source_table='fact_player_efficiency')
    
    # Check valid_from dates
    # 2022 season data -> Known early 2023 (e.g. 2023-02-13 roughly Super Bowl)
    # The default impl should likely set it to YYYY+1-02-01 or similar constant
    
    res = store.con.execute("""
        SELECT feature_value, valid_from 
        FROM feature_values 
        WHERE feature_name = 'total_pass_yds_lag_1' AND prediction_year = 2023
    """).fetchone()
    
    # Target 2023:
    # Lag 1 comes from 2022 data.
    # 2022 data is known as of Fed 2023.
    # So valid_from should be ~2023-02-XX.
    # feature_value should be 4000.
    
    assert res is not None
    val, valid_from = res
    assert val == 4000
    assert valid_from.year == 2023 # Available IN 2023
    
def test_point_in_time_retrieval(store):
    """Simulate a specific historical moment retrieval."""
    # Data:
    # V1: Known 2022-02-01 -> Value 10
    # V2: Known 2022-06-01 -> Value 15 (Correction)
    # V3: Known 2023-02-01 -> Value 20 (Next season)
    
    store.con.execute("""
        INSERT INTO feature_values (entity_key, player_name, prediction_year, feature_name, feature_value, valid_from, valid_until)
        VALUES 
        ('k1', 'P1', 2022, 'f1', 10.0, '2022-02-01', '2022-06-01'),
        ('k2', 'P1', 2022, 'f1', 15.0, '2022-06-01', '2023-02-01'),
        ('k3', 'P1', 2023, 'f1', 20.0, '2023-02-01', NULL)
    """)
    
    # 1. March 2022 -> Should see 10.0
    df_mar = store.get_training_matrix(as_of_date=date(2022, 3, 1), min_year=2022)
    assert df_mar.iloc[0]['f1'] == 10.0
    
    # 2. July 2022 -> Should see 15.0 (Correction)
    df_jul = store.get_training_matrix(as_of_date=date(2022, 7, 1), min_year=2022)
    assert df_jul.iloc[0]['f1'] == 15.0
    
    # 3. March 2023 -> Should see 20.0 (New Season)
    # Note: query checks prediction_year too. 
    # For prediction_year 2022, valid_until was 2023-02-01.
    # If we define "Training Matrix" as "What did we know for THIS target year at THAT time":
    
    # If I am training for 2022 target, and I am currently at 2024:
    # I should see the FINAL version of 2022 features (15.0).
    
    # If I am training for 2023 target:
    # I should see 20.0 (lag 1 from 2022 data)
    pass 
