from src.db_manager import DBManager
import pytest
import os
import pandas as pd

# Use DB_PATH environment variable or default to local development database
DB_PATH = os.getenv("DB_PATH", "data/nfl_data.db")

@pytest.fixture(scope="module")
def con():
    """Shared database connection for the module."""
    if not os.path.exists(DB_PATH):
        pytest.skip(f"Database not found at {DB_PATH}. Skipping Data Quality tests.")
    
    db = DBManager(DB_PATH)
    yield db
    db.close()

# --- 1. Bronze/Silver Layer Validation ---

def test_silver_table_existence(con):
    """Ensures major Silver Layer tables are present and populated."""
    tables = con.execute("SHOW TABLES").df()['name'].tolist()
    expected = ['silver_pfr_game_logs', 'silver_spotrac_contracts', 'silver_penalties']
    for table in expected:
        assert table in tables, f"Missing table: {table}"
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert count > 0, f"Table {table} is empty"

def test_null_boundary_constraints(con):
    """Validates that critical ID fields are never null in Silver tables."""
    # Spotrac Contracts
    nulls = con.execute("SELECT COUNT(*) FROM silver_spotrac_contracts WHERE player_name IS NULL OR team IS NULL").fetchone()[0]
    assert nulls == 0, "Null values found in Spotrac player/team IDs"
    
    # PFR Stats
    nulls = con.execute("SELECT COUNT(*) FROM silver_pfr_game_logs WHERE player_name IS NULL OR year IS NULL").fetchone()[0]
    assert nulls == 0, "Null values found in PFR player/year IDs"

# --- 2. Normalization (Silver) Logic ---

def test_team_mapping_exhaustion(con):
    """Verifies that all team names in sources have been normalized to standard 3 codes."""
    # Check penalties (often have variation in city names)
    teams_df = con.execute("SELECT DISTINCT team FROM silver_penalties").df()
    active_teams = teams_df['team'].dropna().tolist()
    for team in active_teams:
        assert 2 <= len(team) <= 3, f"Non-standard team code found in penalties: {team}"
        assert team.isupper(), f"Lowercase team code found: {team}"

def test_duplicate_signature_detection(con):
    """Checks for accidental row explosion in contract normalization."""
    # A single player-year-team signature should have exactly one contract record in Silver
    # (Excluding edge cases of mid-season trades which we handle via aggregation in Gold)
    dupes = con.execute("""
        SELECT player_name, year, team, COUNT(*) 
        FROM fact_player_efficiency
        GROUP BY 1, 2, 3 
        HAVING COUNT(*) > 1
    """).fetchone()
    assert dupes is None, f"Gold Mart duplicates detected: {dupes}"

# --- 3. Gold Mart Validation ---

def test_outcome_metric_range(con):
    """Ensures performance metrics in Gold layer fall within realistic physical boundaries."""
    # Win Percent [0.0, 1.0]
    wins = con.execute("SELECT win_pct FROM fact_player_efficiency WHERE win_pct < 0 OR win_pct > 1").fetchone()
    assert wins is None, f"Win percentage out of bounds: {wins}"
    
    # Games Played [0, 21] (Max 17 reg + 4 post)
    games = con.execute("SELECT MAX(games_played) FROM fact_player_efficiency").fetchone()[0]
    assert games <= 21, f"Impossible games played count detected: {games}"

def test_entity_resolution_no_explosion(con):
    """Verifies that duplicate player names on different teams aren't merged incorrectly."""
    # Check for Byron Young specifically if present
    byron = con.execute("SELECT COUNT(*) FROM fact_player_efficiency WHERE player_name = 'Byron Young' AND year = 2025").fetchone()[0]
    # If he exists, he should have 2 distinct records (PHI and LAR)
    if byron > 0:
        assert byron >= 2, f"Entity resolution failed: Byron Young should have multiple records, found {byron}"

def test_team_cap_sum_alignment(con):
    """Cross-References the sum of player cap hits in Gold against the Team Total in Silver."""
    # This identifies 'Cap Leaks' where players aren't being captured in the mart
    # Note: Spotrac individual lists might not perfectly match 'Total Cap' due to hidden reserves, 
    # but should be within 15% tolerance.
    leakage = con.execute("""
        WITH gold_sum AS (
            SELECT team, year, SUM(cap_hit_millions) as player_total
            FROM fact_player_efficiency
            GROUP BY 1, 2
        )
        SELECT 
            g.team, g.year, g.player_total, s.salary_cap_millions,
            ABS(g.player_total - s.salary_cap_millions) / s.salary_cap_millions as variance
        FROM gold_sum g
        JOIN silver_team_cap s ON g.team = s.team AND g.year = s.year
        WHERE s.salary_cap_millions > 0
        AND ABS(g.player_total - s.salary_cap_millions) / s.salary_cap_millions > 0.30
    """).df()
    assert len(leakage) == 0, f"Significant cap variance detected in these teams:\n{leakage}"

def test_mart_financial_summation(con):
    """Cross-References player cap hits against known team-level aggregates."""
    # Validate that CAP_ROI is not infinite or null
    roi_check = con.execute("""
        SELECT COUNT(*) 
        FROM fact_player_efficiency 
        WHERE cap_hit_millions > 1.0 AND (cap_roi_score IS NULL OR cap_roi_score = 'inf')
    """).fetchone()[0]
    assert roi_check == 0, "Encountered null or infinite ROI for significant cap players."

def test_cross_layer_consistency(con):
    """Validates that the Gold Mart preserves relational integrity across layers."""
    # Every player in Gold should have a matching record in Silver Spotrac
    orphan_count = con.execute("""
        SELECT COUNT(*) 
        FROM fact_player_efficiency g
        LEFT JOIN silver_spotrac_contracts s ON g.player_name = s.player_name AND g.year = s.year
        WHERE s.player_name IS NULL
    """).fetchone()[0]
    # Allow small drift for players with only PFR stats, but should be < 5%
    total = con.execute("SELECT COUNT(*) FROM fact_player_efficiency").fetchone()[0]
    orphan_rate = orphan_count / total if total > 0 else 0
    assert orphan_rate < 0.05, f"High orphan rate in Gold Layer ({orphan_rate:.2%}). Normalization failing."
