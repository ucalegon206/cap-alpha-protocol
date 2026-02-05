# Cap Alpha Protocol: Production Makefile
# "The Google Standard" for Local Development

.PHONY: all ingest validate features train audit clean help

PYTHON := ./.venv/bin/python
PYTEST := ./.venv/bin/pytest

# Default Target (Full End-to-End)
all: audit

# --- 1. Ingestion (Bronze/Silver) ---
# Depends on raw script changes or manual trigger
data/nfl_belichick.db: scripts/ingest_to_duckdb.py
	@echo "🏈 [Ingest] Hydrating DuckDB (Bronze/Silver)..."
	$(PYTHON) scripts/ingest_to_duckdb.py --year 2024 # Defaults to current for speed, use full backfill separately
	@touch data/nfl_belichick.db

ingest: data/nfl_belichick.db

# --- 2. Quality Gate ---
validate: ingest
	@echo "🛡️ [Validate] Checking Silver Layer Integrity..."
	$(PYTHON) scripts/validate_gold_layer.py

# --- 3. Feature Engineering (Gold) ---
# Only re-run if ingestion changed or feature logic changed
data/gold_features.parquet: data/nfl_belichick.db src/feature_factory.py
	@echo "🏭 [Features] Building Gold Layer..."
	$(PYTHON) src/feature_factory.py

features: data/gold_features.parquet

# --- 4. Model Training (XGBoost) ---
# Only re-train if features changed or model code changed
models/xgb_production.model: data/gold_features.parquet src/train_model.py
	@echo "🧠 [Train] Training Risk Frontier Model..."
	$(PYTHON) src/train_model.py

train: models/xgb_production.model

# --- 6. Visualization (The "Money" Shot) ---
charts: data/nfl_belichick.db
	@echo "🎨 [Viz] Regenerating Executive Charts..."
	$(PYTHON) scripts/generate_all_charts.py
	$(PYTHON) scripts/generate_brand_value_chart.py
	$(PYTHON) scripts/generate_risk_svg.py

# --- 7. Audits & Reliability ---
audit: train charts
	@echo "📜 [Audit] Generating Strategic Intelligence..."
	$(PYTHON) run_pipeline.py --skip-ingest --skip-features --skip-training # Use the orchestrator for reporting layer only

# --- Utilities ---
test:
	@echo "🧪 [Test] Running Integrity Suite..."
	$(PYTEST) tests/test_strategic_engine.py tests/test_data_integrity.py

clean:
	@echo "🧹 [Clean] Removing cache and temporary files..."
	rm -rf __pycache__ .pytest_cache
	rm -f data/gold_features.parquet
	rm -f reports/archive/*

# Full 15-Year Backfill (The "Nuclear Option")
backfill:
	@echo "☢️ [Backfill] Executing Full 15-Year Protocol..."
	$(PYTHON) scripts/backfill_dead_cap.py --start 2011 --end 2025
	for y in {2011..2025}; do $(PYTHON) scripts/ingest_to_duckdb.py --year $$y; done

help:
	@echo "Cap Alpha Protocol Build System"
	@echo "-------------------------------"
	@echo "make all       -> Run full pipeline (incremental)"
	@echo "make ingest    -> Update DuckDB (Bronze/Silver)"
	@echo "make features  -> Rebuild Gold Layer"
	@echo "make train     -> Retrain Execution Model"
	@echo "make charts    -> Regenerate SVG Visualizations"
	@echo "make audit     -> Generate MD Reports"
	@echo "make test      -> Run Pytest Suite"
	@echo "make backfill  -> Run 15-year History Scrape"
