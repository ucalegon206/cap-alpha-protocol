# --- Environment Configuration ---
DB_NAME := nfl_data.db
DB_FILE := data/$(DB_NAME)

# Sacrosanct Runtime (Bypasses local macOS filesystem locks)
SACROSANCT_ENV := /tmp/nfl_production_env
PYTHON := PYTHONDONTWRITEBYTECODE=1 $(SACROSANCT_ENV)/bin/python
PYTEST := PYTHONDONTWRITEBYTECODE=1 $(SACROSANCT_ENV)/bin/pytest
PRECOMMIT := $(SACROSANCT_ENV)/bin/pre-commit

# --- 1. Ingestion (Bronze/Silver) ---
# Depends on raw script changes or manual trigger
$(DB_FILE): scripts/ingest_to_duckdb.py
	@echo "🏈 [Ingest] Hydrating DuckDB (Bronze/Silver) for 2025..."
	$(PYTHON) scripts/ingest_to_duckdb.py --year 2025
	@touch $(DB_FILE)

ingest: $(DB_FILE)

# --- 2. Quality Gate ---
validate: ingest
	@echo "🛡️ [Validate] Checking Silver Layer Integrity..."
	$(PYTHON) scripts/validate_gold_layer.py

# --- 3. Feature Engineering (Gold) ---
# Only re-run if ingestion changed or feature logic changed
data/gold_features.parquet: $(DB_FILE) src/feature_factory.py
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
charts: $(DB_FILE)
	@echo "🎨 [Viz] Regenerating Executive Charts..."
	$(PYTHON) scripts/generate_all_charts.py
	$(PYTHON) scripts/generate_brand_value_chart.py
	$(PYTHON) scripts/generate_risk_svg.py

# --- 7. Audits & Reliability ---
audit: train charts
	@echo "📜 [Audit] Generating Strategic Intelligence..."
	$(PYTHON) run_pipeline.py --skip-ingest --skip-features --skip-training # Use the orchestrator for reporting layer only

# --- Utilities & QA ---
test-verbose:
	@echo "🧪 [Test] Running Verbose Integrity Suite..."
	$(PYTEST) -v tests/test_strategic_engine.py tests/test_gold_integrity.py tests/test_dag_integrity.py tests/test_data_quality.py

test-quick:
	@echo "🧪 [Test] Running Quick Suite (Skipping Heavy Simulations)..."
	$(PYTEST) -m "not slow" tests/

test-coverage:
	@echo "🧪 [Test] Running Coverage Report..."
	$(PYTHON) -m pytest --cov=src --cov=scripts tests/ --cov-report=html
	@echo "📊 Report generated in htmlcov/index.html"

# --- Code Quality ---
format:
	@echo "✨ [Format] Running Black and isort..."
	$(PYTHON) -m black .
	$(PYTHON) -m isort .

format-check:
	@echo "🔍 [Check] Verifying Formatting..."
	$(PYTHON) -m black --check .
	$(PYTHON) -m isort --check-only .

lint:
	@echo "🧼 [Lint] Running flake8..."
	$(PYTHON) -m flake8 src/ scripts/ tests/

clean:
	@echo "🧹 [Clean] Removing cache and temporary files..."
	rm -rf __pycache__ .pytest_cache htmlcov .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -f data/gold_features.parquet
	rm -f reports/archive/*

clean-venv:
	@echo "☢️ [Nuclear] Removing virtual environment..."
	rm -rf .venv

doctor:
	@echo "🩺 [Doctor] Checking Environment Health..."
	@ls -d /tmp/nfl_production_env >/dev/null 2>&1 && echo "✅ Global Sacrosanct Runtime found." || echo "❌ Global runtime MISSING. Run 'make setup'"
	@ls -d .venv >/dev/null 2>&1 && echo "✅ Local .venv found (Locked: $$(ls -ld .venv | cut -d' ' -f1))" || echo "ℹ️ Local .venv missing."
	@$(PYTHON) --version || echo "❌ Python executable not working."
	@echo "🔍 Checking test file parity..."
	@test -f tests/test_strategic_engine.py && echo "✅ test_strategic_engine.py present" || echo "❌ test_strategic_engine.py MISSING"
	@test -f tests/test_gold_integrity.py && echo "✅ test_gold_integrity.py present" || echo "❌ test_gold_integrity.py MISSING"
	@test -f tests/test_dag_integrity.py && echo "✅ test_dag_integrity.py present" || echo "❌ test_dag_integrity.py MISSING"
	@$(PYTHON) -c "import duckdb; print(f'✅ DuckDB version: {duckdb.__version__}')" 2>/dev/null || echo "❌ DuckDB module MISSING."

# Default Target (Full End-to-End)
all: refresh-2025 audit

refresh-2025:
	@echo "🔄 [Refresh] Forcing 2025 Re-Ingest..."
	rm -f $(DB_FILE)
	$(MAKE) audit

# Full 15-Year Backfill (The "Nuclear Option")
backfill:
	@echo "☢️ [Backfill] Executing Full 15-Year Protocol..."
	$(PYTHON) scripts/backfill_dead_cap.py --start 2011 --end 2025
	for y in {2011..2025}; do $(PYTHON) scripts/ingest_to_duckdb.py --year $$y; done

sacrosanct-test:
	@echo "🧪 [Sacrosanct] Running Integrity Suite in Secured Runtime..."
	@test -f $(DB_FILE) && (cp $(DB_FILE) /tmp/nfl_sacrosanct_test.db || echo "⚠️ Using existing /tmp db due to lock") || echo "ℹ️ DB_FILE missing, using /tmp db"
	DB_PATH=/tmp/nfl_sacrosanct_test.db PYTHONPATH=. PYTHONDONTWRITEBYTECODE=1 $(SACROSANCT_PYTEST) tests/test_strategic_engine.py tests/test_gold_integrity.py tests/test_data_quality.py

setup:
	@echo "🛠️ [Setup] Reinforcing Global Runtime in $(SACROSANCT_ENV)..."
	python3 -m venv $(SACROSANCT_ENV)
	PYTHONDONTWRITEBYTECODE=1 $(SACROSANCT_ENV)/bin/pip install --upgrade pip --no-cache-dir
	PYTHONDONTWRITEBYTECODE=1 $(SACROSANCT_ENV)/bin/pip install -r requirements.txt --no-cache-dir
	PYTHONDONTWRITEBYTECODE=1 $(SACROSANCT_ENV)/bin/pip install pre-commit pytest pytest-cov --no-cache-dir
	@echo "✅ Global Setup Complete."

test:
	@echo "🧪 [Test] Running Integrity Suite with DB Isolation..."
	@$(PYTEST) --version >/dev/null 2>&1 || (echo "❌ pytest not found in $(SACROSANCT_ENV). Run 'make setup'"; exit 1)
	@echo "💧 [Hydrate] Provisioning temporary test database..."
	PYTHONPATH=. DB_PATH=/tmp/test_runtime.db $(PYTHON) scripts/ingest_to_duckdb.py --year 2023
	PYTHONPATH=. DB_PATH=/tmp/test_runtime.db $(PYTHON) scripts/ingest_to_duckdb.py --year 2025
	DB_PATH=/tmp/test_runtime.db $(PYTEST) tests/test_strategic_engine.py tests/test_gold_integrity.py tests/test_dag_integrity.py tests/test_data_quality.py

help:
	@echo "Cap Alpha Protocol Build System"
	@echo "-------------------------------"
	@echo "make all              -> Run full pipeline (incremental)"
	@echo "make ingest           -> Update DuckDB (Bronze/Silver)"
	@echo "make features         -> Rebuild Gold Layer"
	@echo "make train            -> Retrain Execution Model"
	@echo "make charts           -> Regenerate SVG Visualizations"
	@echo "make audit            -> Generate MD Reports"
	@echo "make test             -> Run Pytest Suite"
	@echo "make sacrosanct-test  -> Run tests in secured /tmp env (Bypass OS locks)"
	@echo "make test-coverage    -> Generate HTML Coverage Report"
	@echo "make lint             -> Run Code Linting"
	@echo "make format           -> Auto-format Code"
	@echo "make backfill         -> Run 15-year History Scrape"
	@echo "make doctor           -> Check environment health"
