# Weekly Pipeline Testing Guide

## Overview

This guide documents the test suite for the weekly NFL dead money pipeline scheduled runs.

## Test Categories

### 1. **Unit Tests** (Fast, No External Dependencies)
- Location: `tests/test_dead_money_validator.py`, `tests/test_salary_cap_validation.py`
- Run with: `make test` or `make test-quick`
- Purpose: Validate core logic (validators, reference data)
- **2 existing files, 20+ tests**

### 2. **Integration Tests** (Requires Network)
- Location: `tests/test_scraper_integration.py`
- Run with: `make test-integration`
- Purpose: Test scrapers with live/mock data
- Tests:
  - Spotrac scraper end-to-end (team cap, player salaries)
  - PFR scraper (rosters, tables)
  - Error handling (timeouts, missing tables)
  - Data quality gates

**Critical for:** Ensuring scrapers work when Spotrac/PFR page structures change

### 3. **Data Freshness Tests** (Weekly Run Readiness)
- Location: `tests/test_data_freshness.py`
- Run with: `make test-weekly`
- Purpose: Validate data recency and completeness
- Tests:
  - Current year data exists
  - Files are <7 days old
  - Processed data complete for past 3 years
  - All 32 teams present
  - Parquet sidecars generated
  - DuckDB database exists

**Critical for:** Detecting stale data in scheduled runs

### 4. **Pipeline Idempotency Tests** (Data Integrity)
- Location: `tests/test_pipeline_idempotency.py`
- Run with: `make test-weekly`
- Purpose: Ensure re-runs don't corrupt data
- Tests:
  - Normalization produces same output on re-run
  - No duplicate records after multiple runs
  - Referential integrity between tables
  - Primary keys have no nulls
  - Year values within reasonable range
  - CSV column names stable

**Critical for:** Safe weekly re-runs without data corruption

### 5. **Year Parameterization Tests** (Future Year Support)
- Location: `tests/test_year_parameterization.py`
- Run with: `make test-weekly`
- Purpose: Test pipeline with future years (e.g., 2026)
- Tests:
  - Scraper accepts future years without crashing
  - Normalization handles missing future data gracefully
  - Year validation (rejects pre-2011, negative)
  - Current year detection
  - CLI year override
  - Backfill range validation
  - Papermill parameter injection

**Critical for:** Running pipeline for upcoming seasons

### 6. **Merge Quality Tests** (Roster+Salary Fuzzy Matching)
- Location: `tests/test_merge_quality.py`
- Run with: `make test`
- Purpose: Validate roster+salary merge logic
- Tests:
  - Exact name matches return high scores
  - Jr/Sr/II suffix handling
  - Poor matches rejected (threshold)
  - Team filtering prevents cross-team matches
  - Merge preserves all roster records
  - Unmatched rosters have null salaries
  - No duplicate records
  - Match rate ≥50%
  - Salary values reasonable (0-100M)

**Critical for:** Accurate financial data attribution

## Test Execution Commands

```bash
# Fast unit tests only (default, runs in CI)
make test

# Critical tests for weekly pipeline
make test-weekly

# Integration tests (requires network)
make test-integration

# All tests (unit + integration + slow)
make test-all

# Tests with coverage report
make test-coverage
```

## CI/CD Integration

### GitHub Actions Workflow (`.github/workflows/tests.yml`)

**Jobs:**
1. **test** (Python 3.10/3.11 matrix)
   - Runs: `make test` (unit tests only, fast)
   - Uploads coverage XML to Codecov
   
2. **lint** (code quality)
   - black, isort, flake8
   - Blocks merge on failure
   
3. **dbt_test** (schema validation)
   - Runs: `dbt deps && dbt compile && dbt test`
   - Validates staging/marts schemas

**Weekly tests NOT in CI** (require live data/network):
- Integration tests
- Data freshness tests

## Pre-Production Checklist

Before deploying weekly scheduled pipeline, run:

```bash
# 1. Validate all unit tests pass
make test-verbose

# 2. Run weekly pipeline tests
make test-weekly

# 3. Run integration tests (if network available)
make test-integration

# 4. Validate data quality
make validate

# 5. Run dbt tests
make dbt-test

# 6. Check coverage
make test-coverage
```

## Test Markers

Use pytest markers to filter tests:

```bash
# Unit tests only (fast)
pytest tests/ -m "unit"

# Exclude slow tests
pytest tests/ -m "not slow"

# Integration tests only
pytest tests/ -m "integration"

# Weekly pipeline tests
pytest tests/ -m "weekly"
```

Markers defined in [pytest.ini](pytest.ini):
- `slow`: Long-running tests
- `integration`: Requires network/live data
- `unit`: Fast, no external dependencies
- `weekly`: Critical for scheduled runs
- `data_quality`: Data validation tests
- `validator`: Dead money validator tests

## Coverage Goals

| Component | Current | Target |
|-----------|---------|--------|
| Overall | 9% | 70% |
| Scrapers | 0% | 60% |
| Normalization | 15% | 80% |
| Validators | 85% | 90% |
| Merge Logic | 0% | 75% |

## Adding New Tests

### For New Scrapers
1. Add integration test to `tests/test_scraper_integration.py`
2. Test with live data (mark as `@pytest.mark.integration`)
3. Test error handling (timeouts, missing data)
4. Add data quality validation

### For New Transforms
1. Add unit test (fast, use fixtures)
2. Add idempotency test to `tests/test_pipeline_idempotency.py`
3. Test edge cases (empty data, nulls, duplicates)

### For New Data Sources
1. Add freshness test to `tests/test_data_freshness.py`
2. Test completeness (years, teams)
3. Test recency (<7 days)

## Known Issues

1. **Integration tests require network**: Can't run in restricted CI environments
2. **Slow tests**: Scraping tests take 30-60s each (mark with `@pytest.mark.slow`)
3. **Future year data**: 2026 tests will fail until Spotrac publishes data

## Troubleshooting

### "Test failed: No team cap files found"
- Run scrapers first: `PYTHONPATH=. ./.venv/bin/python src/spotrac_scraper_v2.py backfill 2023 2024`

### "Match rate below 50%"
- Check roster+salary year alignment
- Verify Spotrac player salary data scraped
- Review fuzzy matching threshold (0.75)

### "Data not fresh (>7 days old)"
- Re-run weekly pipeline: `airflow dags trigger nfl_dead_money_pipeline`
- Or manually: `make scrape && make normalize`

## References

- [TESTING.md](../TESTING.md): General testing philosophy
- [QUICKSTART_TESTING.md](../QUICKSTART_TESTING.md): Quick testing guide
- [pytest.ini](../pytest.ini): Test configuration
- [Makefile](../Makefile): Test commands
