# Quick Start: CI/CD & Testing

## Run Tests Locally (5 seconds)

```bash
make test
```

## View Test Results with Coverage

```bash
make test-coverage
```

## Format & Lint Code

```bash
make format          # Auto-fix formatting
make format-check    # Check without fixing
make lint            # Check all linting issues
```

## Install Pre-commit Hooks (One-time Setup)

```bash
make install-hooks
```

After this, tests run automatically before every commit.

## Run Dead Money Validator

```bash
make validate          # Quick run
make validate-verbose  # Detailed output
```

## GitHub Actions

Tests automatically run on:
- ✅ Every push to `main` or `develop`
- ✅ Every pull request
- ✅ Every local commit (if pre-commit installed)

View results at: https://github.com/ucalegon206/nfl-dead-money/actions

## Test Structure

**File:** `tests/test_dead_money_validator.py`

Tests include:
- Synthetic player detection
- Team vs player reconciliation
- Year-over-year consistency
- Data file availability

**Run specific test:**
```bash
pytest tests/test_dead_money_validator.py::TestDeadMoneyValidation::test_synthetic_players -v
```

## What Gets Tested

1. **Local Pre-commit** (before every commit)
   - Code formatting (black)
   - Import sorting (isort)
   - Linting (flake8)
   - File integrity checks

2. **Pytest Suite** (local + GitHub Actions)
   - DeadMoneyValidator tests (8 cases)
   - Data availability checks

3. **Airflow Integration** (during DAG execution)
   - Dead money validation task
   - Slack alerts on warnings

## All Commands

See [TESTING.md](TESTING.md) for complete documentation.

```bash
make help          # Show all available commands
```
