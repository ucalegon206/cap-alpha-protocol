import duckdb
import pytest
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = "data/nfl_data.db"

def test_gold_layer_build_integrity():
    """
    Ensures the Gold Layer can be built without BinderException (casting issues)
    or ParserException (syntax issues).
    """
    con = duckdb.connect(DB_PATH)
    
    try:
        # We don't want to actually overwrite the production table here if we can avoid it,
        # but create_gold_layer does 'CREATE OR REPLACE'.
        # For a test, we'll try to execute the core logic into a temp table.
        from scripts.ingest_to_duckdb import create_gold_layer
        
        # We can't easily run create_gold_layer on a temp table without modifying it,
        # but we can verify it runs on the current DB.
        create_gold_layer()
        logger.info("✓ Gold Layer build successful (no SQL errors)")
        
    except Exception as e:
        pytest.fail(f"Gold Layer build failed with error: {e}")
    finally:
        con.close()

def test_no_row_explosion():
    """
    Checks that core players don't have excessive rows (sign of bad joins).
    """
    con = duckdb.connect(DB_PATH)
    
    # Check a few specific players known to cause issues
    players = ["Josh Sweat", "Marvin Harrison Jr."]
    for player in players:
        count = con.execute(f"SELECT COUNT(*) FROM fact_player_efficiency WHERE player_name = '{player}' AND year = 2025").fetchone()[0]
        # A player might be on two teams (max 2 rows usually), but 16 is definitely an explosion.
        assert count <= 2, f"Row explosion detected for {player}: {count} rows. Expected <= 2."
    
    con.close()

def test_penalty_linkage():
    """
    Ensures penalties are actually linking to the gold layer.
    """
    con = duckdb.connect(DB_PATH)
    penalty_link_count = con.execute("SELECT COUNT(*) FROM fact_player_efficiency WHERE year = 2025 AND total_penalty_yards > 0").fetchone()[0]
    assert penalty_link_count > 0, "No players in the Gold Layer have penalty data for 2025."
    con.close()

if __name__ == "__main__":
    # Run simple check if executed directly
    con = duckdb.connect(DB_PATH)
    print("Running integrity checks...")
    try:
        test_gold_layer_build_integrity()
        test_no_row_explosion()
        test_penalty_linkage()
        print("ALL INTEGRITY CHECKS PASSED")
    except Exception as e:
        print(f"INTEGRITY CHECK FAILED: {e}")
    finally:
        con.close()
