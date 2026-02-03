#!/bin/bash
set -e

echo "======================================================="
echo "STARTING E2E PRODUCTION PIPELINE TEST"
echo "======================================================="

# 1. SCRAPING
echo "-------------------------------------------------------"
echo "[1/6] Running Scrapers (Year=2025, Week=1)..."
# Simulate a specific week run to test parameter passing
.venv/bin/python src/run_historical_scrape.py --years 2025 --week 1 --source all --force

# 2. INGESTION
echo "-------------------------------------------------------"
echo "[2/6] Ingesting Data to DuckDB (Year=2025)..."
.venv/bin/python scripts/ingest_to_duckdb.py --year 2025 --week 1

# 3. FEATURE ENGINEERING
echo "[3/5] Building Feature Matrix..."
.venv/bin/python src/feature_factory.py

# 4. TRAINING
echo "[4/5] Training Risk Model..."
.venv/bin/python src/train_model.py

# 5. REPORTING
echo "[5/5] Generating Financial Lift Report..."
.venv/bin/python scripts/financial_lift_report.py

# 6. VERIFICATION
echo "-------------------------------------------------------"
echo "[6/6] Verifying Artifacts..."

# Check Gold Layer
GOLD_COUNT=$(.venv/bin/python -c "import duckdb; con = duckdb.connect('data/nfl_data.db'); print(con.execute('SELECT COUNT(*) FROM fact_player_efficiency WHERE year = 2025').fetchone()[0])")
if [ "$GOLD_COUNT" -gt 0 ]; then
    echo "  ✓ Gold Layer populated for 2025: $GOLD_COUNT rows"
else
    echo "  ✗ FAILURE: Gold Layer empty for 2025"
    exit 1
fi

# Check Feature Matrix (Staging)
FEAT_COUNT=$(.venv/bin/python -c "import duckdb; print(duckdb.connect('data/nfl_data.db').execute('SELECT COUNT(*) FROM staging_feature_matrix').fetchone()[0])")
if [ "$FEAT_COUNT" -gt 0 ]; then
    echo "  ✓ Feature Matrix populated: $FEAT_COUNT rows"
else
    echo "  ✗ FAILURE: Feature Matrix empty"
    exit 1
fi

# Check Report Output (Can check if command exited 0, which it did if we are here, but let's check explicit output if possible)
# The report script prints to stdout, so we assume success if step 5 passed.

echo "======================================================="
echo "✓ E2E TEST PASSED: All pipeline stages completed successfully."
echo "======================================================="
