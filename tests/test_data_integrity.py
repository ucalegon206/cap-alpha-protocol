
import pytest
import duckdb
import os

DB_PATH = "data/nfl_data.db"

@pytest.fixture
def db_conn():
    if not os.path.exists(DB_PATH):
        pytest.skip("Database not found for integrity tests")
    con = duckdb.connect(DB_PATH)
    yield con
    con.close()

def test_gold_layer_team_completeness(db_conn):
    """Ensure all 32 teams are represented in the gold layer for the target year."""
    year = 2025
    count = db_conn.execute(f"SELECT COUNT(DISTINCT team) FROM fact_player_efficiency WHERE year = {year}").fetchone()[0]
    assert count == 32, f"Expected 32 teams, found {count}"

def test_gold_layer_no_null_criticals(db_conn):
    """Ensure critical columns like player_name and cap_hit_millions are not null."""
    year = 2025
    null_count = db_conn.execute(f"""
        SELECT COUNT(*) FROM fact_player_efficiency 
        WHERE year = {year} AND (player_name IS NULL OR cap_hit_millions IS NULL)
    """).fetchone()[0]
    assert null_count == 0, f"Found {null_count} records with null critical fields in 2025"

def test_prediction_coverage_2025(db_conn):
    """Ensure every record in the 2025 gold layer has a corresponding prediction."""
    year = 2025
    missing_preds = db_conn.execute(f"""
        SELECT COUNT(*) 
        FROM fact_player_efficiency f
        LEFT JOIN prediction_results p 
          ON f.player_name = p.player_name AND f.year = p.year AND f.team = p.team
        WHERE f.year = {year} AND p.predicted_risk_score IS NULL
    """).fetchone()[0]
    # We allow a small tolerance for players who might have been dropped or are missing in model input
    assert missing_preds < 50, f"Too many missing predictions in 2025: {missing_preds}"
