# AI Coding Agent Instructions - NFL Dead Money Analysis

## Project Overview

**Purpose**: Analyze NFL salary cap dead money (money paid to non-contributing players) to identify patterns and predictability.

**Tech Stack**: Python 3.x | Apache Airflow (CeleryExecutor) | dbt (DuckDB backend) | Pytest | Plotly

**Architecture**: Data pipeline with 4 layers (scraping → staging → normalization → marts) + CI/CD + validation

---

## Critical Workflows & Commands

### Testing & Validation (Use Makefile)
```bash
make test              # Run full pytest suite (8 tests)
make test-coverage     # With HTML coverage report
make validate          # Run DeadMoneyValidator (cross-validation of team vs player sums)
make format            # Auto-fix code (black + isort)
make install-hooks     # Pre-commit hooks (runs tests on commit)
```

### Data Pipeline
```bash
# From workspace root:
PYTHONPATH=. ./.venv/bin/python src/spotrac_scraper_v2.py backfill 2015 2024  # Scrape team caps
PYTHONPATH=. ./.venv/bin/python src/historical_scraper.py                     # Scrape player rankings
./.venv/bin/dbt seed --project-dir ./dbt --profiles-dir ./dbt                 # Load seeds
./.venv/bin/dbt run --project-dir ./dbt --profiles-dir ./dbt                  # Transform data
```

### Notebooks & Analysis
- **Production**: `notebooks/07_production_dead_money_dashboard.ipynb` (loads scraped data, visualizations)
- **Decision Tree Analysis**: Cells added for predicting dead money by position/team (scikit-learn DecisionTreeRegressor)

---

## Architecture Layers & Data Flow

### 1. **Scraping** → `data/raw/`
- `spotrac_scraper_v2.py`: Team cap data (team, active_cap, dead_money, cap_space)
- `historical_scraper.py`: Player rankings by year
- Files timestamped: `spotrac_team_cap_2024_20251218.csv`

### 2. **Staging** → `data/staging/`
- Raw CSVs validated & loaded via `dbt seed` + `src/ingestion.py`
- Schema: lowercase, standardized column names

### 3. **Normalization** → `data/processed/compensation/`
- Python transforms in `src/normalization.py`
- Key files: `player_dead_money.csv` (181 records with synthetic player flag), `team_dead_money_by_year.csv`

### 4. **dbt Marts** → DuckDB `nfl_dead_money.duckdb`
- `mart_player_cap_impact`, `mart_team_summary` (fact tables)
- Materialized as tables, schema `marts`

### 5. **Validation & Alerting**
- `DeadMoneyValidator`: Cross-validates team vs player sums (CSV-based, no DB locks)
- `Airflow DAG` with `slack_on_task_failure()` callback (all 12+ operators wired)
- GitHub Actions: Pytest runs on every push (Python 3.10+)

---

## Key Conventions & Patterns

### Data Quality Approach
- **Synthetic Players**: Dataset includes synthetic/placeholder names (e.g., "Von Walker 5"). Flag with `is_king=False` prefix; **retained by design** (not filtered).
- **Team vs Player Reconciliation**: `DeadMoneyValidator.test_team_player_reconciliation_csv()` allows ±5% variance (legitimate due to accounting differences).
- **Salary Cap Reference**: `src/salary_cap_reference.py` hardcodes official NFL caps (2011-2024) for validation. Base cap ≠ Spotrac "Total Cap" (latter includes carryover).

### Testing Philosophy
- **Pytest fixtures** in `tests/test_*.py` load real CSVs from `data/processed/` (not mocked)
- **Validator tests**: Data file availability checks + logic tests (e.g., cap components, dead % ranges)
- **Pre-commit**: Runs pytest before every commit (configurable in `.pre-commit-config.yaml`)

### Naming Conventions
- Variables: `snake_case`; CSV columns: `snake_case` (converted from Spotrac title case)
- Team codes: Normalize via `TEAM_CODE_MAP` (e.g., `TAM` → `TB`, `SFO` → `SF`)
- Files: Descriptive names with timestamps in raw data (`spotrac_team_cap_{year}_{timestamp}.csv`)

### Error Handling
- Scrapers log detailed progress; raise `DataQualityError` on validation failure (not silent)
- Airflow fails task on non-zero exit code; Slack alert fires automatically
- Validator exits code 0 even with warnings (non-blocking design)

---

## Integration Points & Dependencies

### External Data Sources
1. **Spotrac** (`www.spotrac.com/nfl/cap/`): Selenium-scraped team caps + player rankings
2. **Pro Football Reference**: Rosters (via `historical_scraper.py`)
3. **NFL.com**: Official salary caps (hardcoded reference in `salary_cap_reference.py`)

### Cross-Component Communication
- **Airflow → dbt**: Bash operators trigger `dbt seed/run`; logs captured
- **Airflow → Slack**: `slack_on_task_failure()` fires on task error (requires `SLACK_WEBHOOK_URL` env var)
- **dbt → DuckDB**: Local `.duckdb` file (no separate database)

### CI/CD Pipeline
- **GitHub Actions** (`.github/workflows/tests.yml`): Pytest + coverage on push/PR
- **Pre-commit**: Black, isort, flake8 run locally before commit
- **Linting**: Continue-on-error in GH Actions (non-blocking)

---

## Common Tasks & Examples

### Add a New Validation Test
1. Add test function to `tests/test_dead_money_validator.py` (pytest class-based)
2. Use pytest fixtures to load CSVs: `dead_money_df = pd.read_csv('data/processed/compensation/player_dead_money.csv')`
3. Run locally: `make test-verbose`; commit triggers pre-commit hooks

### Investigate Data Anomaly
1. Check `docs/SALARY_CAP_ANOMALY_INVESTIGATION.md` for known issues (e.g., 2016 CLE, 2019 SF)
2. Run `make validate-verbose` to see cross-validation results
3. For salary cap: Compare Spotrac total vs official NFL cap via `src/salary_cap_reference.py` (±15% tolerance expected due to carryover)

### Update Salary Cap Reference
1. Edit `src/salary_cap_reference.py` dict (official caps from NFL.com)
2. Update `tests/test_salary_cap_validation.py` expectations
3. Run: `make test && make validate`

### Add Notebook Analysis
1. Work in `notebooks/07_production_dead_money_dashboard.ipynb` (production analysis)
2. Load data from `data/processed/compensation/` (use absolute imports with `sys.path`)
3. Save outputs to `notebooks/outputs/`

---

## Important Context & Gotchas

- **Spotrac "Total Cap" ≠ Official NFL Salary Cap**: Spotrac includes carryover credits; base cap is fixed per year. Individual teams can vary ±15-20%.
- **CSV-based validation**: DeadMoneyValidator uses processed CSVs, not dbt models (intentional: no DB locks, portable).
- **Synthetic data**: 82.9% of player records are synthetic (detected by numbered name suffixes). Kept for analysis (not filtered).
- **No Postgres**: Project uses DuckDB locally; Postgres is optional/skipped in setup.
- **Airflow local mode**: CeleryExecutor configured but may fall back to SequentialExecutor if Redis unavailable.

---

## Quick Reference

| Component | Location | Key Files |
|-----------|----------|-----------|
| Scraping | `src/` | `spotrac_scraper_v2.py`, `historical_scraper.py` |
| Staging | `src/` | `ingestion.py`, `pipeline_tasks.py` |
| Validation | `src/` | `dead_money_validator.py`, `data_quality_tests.py` |
| dbt Models | `dbt/models/` | staging, intermediate, marts layers |
| Tests | `tests/` | `test_dead_money_validator.py`, `test_salary_cap_validation.py` |
| Pipeline | `dags/` | `nfl_dead_money_pipeline.py` (Airflow DAG) |
| Reference | `docs/` | `SALARY_CAP_SOURCES.md`, `SALARY_CAP_ANOMALY_INVESTIGATION.md` |

---

## Execution Guidelines for AI Agents

- **Start with `make` commands**: Most workflows available via `make help`
- **Check data first**: Load `data/processed/compensation/*.csv` to understand structure
- **Reference existing patterns**: Look at `src/dead_money_validator.py` for validation structure, `tests/test_*.py` for test patterns
- **Respect data retention policy**: Keep synthetic players in dataset; flag them, don't filter
- **Test locally before pushing**: `make test && make validate` before git commit
- **Document anomalies**: Add to `docs/SALARY_CAP_ANOMALY_INVESTIGATION.md` if discovering new issues
- **Use pytest fixtures**: Don't hardcode paths; load data via fixture with Path resolution
