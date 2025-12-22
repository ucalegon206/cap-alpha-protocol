# CI/CD & Automated Testing

This document describes the automated testing setup for the NFL Dead Money pipeline.

## Overview

The project uses multiple layers of automated testing:

1. **Local Pre-commit Hooks** - Run before every commit
2. **Pytest Unit Tests** - Run during development and CI/CD
3. **GitHub Actions Workflow** - Runs on every push and pull request
4. **Airflow Integration Tests** - Run as part of the production DAG

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
pip install pytest pytest-cov pre-commit
```

### 2. Install Pre-commit Hooks

```bash
pre-commit install
```

This will run code formatting, linting, and pytest before every commit.

## Running Tests Locally

### Quick Test Run

```bash
make test
```

### Verbose Output

```bash
make test-verbose
```

### Test Coverage Report

```bash
make test-coverage
```

This generates an HTML report in `htmlcov/index.html`.

### Run Only Unit Tests (Skip Slow Tests)

```bash
make test-quick
```

## Code Quality

### Format Code (Black + isort)

```bash
make format
```

### Check Formatting (Without Making Changes)

```bash
make format-check
```

### Run All Linters

```bash
make lint
```

This runs:
- **flake8** - PEP8 style checking
- **black** - Code formatting
- **isort** - Import sorting

## Validation

### Run Dead Money Validator

```bash
make validate
```

### Verbose Validator Output

```bash
make validate-verbose
```

## Test Suite Structure

### Test Files

- **`tests/test_dead_money_validator.py`** - DeadMoneyValidator tests
  - Synthetic player detection
  - Team vs player reconciliation
  - Year-over-year consistency
  - Data file availability

### Test Markers

You can run specific test categories:

```bash
# Run only data quality tests
pytest tests/ -m "data_quality"

# Run only unit tests
pytest tests/ -m "unit"

# Skip slow tests
pytest tests/ -m "not slow"
```

## GitHub Actions Workflow

The workflow (`.github/workflows/tests.yml`) runs on:

- Every push to `main` or `develop`
- Every pull request to `main`
- When changes are made to `src/`, `tests/`, or `requirements.txt`

### Workflow Jobs

1. **Test** (Matrix: Python 3.10, 3.11)
   - Install dependencies
   - Run pytest with coverage
   - Run dead money validator
   - Upload coverage to Codecov

2. **Lint** (Python 3.11)
   - Check Black formatting
   - Check import sorting
   - Run flake8 linting

### Viewing Results

GitHub Actions results are visible in:
- Pull request checks
- Repository "Actions" tab
- Commit status checks

## Integration with Airflow DAG

The DeadMoneyValidator is integrated into the Airflow pipeline as a separate task:

```python
dead_money_validation_task = PythonOperator(
    task_id="dead_money_validation",
    python_callable=validate_dead_money,
    on_failure_callback=slack_on_task_failure(),
)
```

This task:
- Runs after data normalization
- Validates team vs player dead money sums
- Reports synthetic player detection
- Checks year-over-year consistency
- **Does NOT block the pipeline** (uses WARN status, not FAIL)
- Sends Slack notification if any validation warnings occur

## Pre-commit Hooks

The `.pre-commit-config.yaml` file configures:

1. **Code Formatting**
   - Black (Python formatting)
   - isort (Import sorting)

2. **Linting**
   - flake8 (Style checking)
   - yamllint (YAML files)
   - bandit (Security checks)

3. **File Checks**
   - Trailing whitespace
   - EOF newlines
   - JSON/YAML validity
   - Large file detection

4. **Type Checking** (Manual Stage)
   ```bash
   pre-commit run --hook-stage manual
   ```

## Continuous Deployment Considerations

### Before Deploying to Production

1. All tests pass locally: `make test`
2. Code is formatted: `make format-check`
3. Validator shows acceptable warnings: `make validate`
4. All changes are committed: `git status`
5. GitHub Actions workflow passed on main branch

### Monitoring

- Check GitHub Actions for CI/CD failures
- Monitor Slack alerts from Airflow DAG
- Review dead money validator warnings for data quality issues
- Check codecov.io for coverage trends

## Troubleshooting

### Pre-commit Hook Issues

**Hooks not running on commit:**
```bash
pre-commit install
```

**Force run hooks on all files:**
```bash
make run-hooks
```

### Test Failures

**Check test output:**
```bash
make test-verbose
```

**Check coverage report:**
```bash
make test-coverage
```

**Debug specific test:**
```bash
pytest tests/test_dead_money_validator.py::TestDeadMoneyValidation::test_synthetic_players -v
```

### Data Not Found Errors

Ensure processed data exists:
```bash
ls -la data/processed/compensation/
```

If missing, run the pipeline first or use sample data for testing.

### GitHub Actions Failures

1. Check the "Actions" tab for error logs
2. Ensure all dependencies are in `requirements.txt`
3. Verify Python version compatibility (3.10+)
4. Check environment variable setup (PYTHONPATH, etc.)

## Best Practices

1. **Always run tests locally before pushing:**
   ```bash
   make test && make lint
   ```

2. **Format code before committing:**
   ```bash
   make format
   ```

3. **Review dead money validator output:**
   ```bash
   make validate-verbose
   ```

4. **Use meaningful commit messages that reference tests:**
   ```
   feat: add reconciliation tolerance parameter
   - Added configurable tolerance_pct to validator
   - Tests check tolerance = 5% (± variance)
   - Validates team vs player sums
   ```

5. **Monitor CI/CD pipeline health:**
   - Check "Actions" tab regularly
   - Subscribe to GitHub notifications
   - Monitor Slack for DAG failures

## Next Steps

- [ ] Set up Codecov.io integration for coverage tracking
- [ ] Add database integrity tests (dbt Expectations)
- [ ] Set up performance benchmarks for data processing
- [ ] Configure automatic deployment on successful CI/CD runs
- [ ] Add E2E tests for full pipeline execution
