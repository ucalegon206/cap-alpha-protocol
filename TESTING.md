# CI/CD & Automated Testing

**Status**: Production (v2.0)
**Core Principle**: "Local Libs" Strategy (`./libs`) to bypass macOS environment restrictions.

## 1. Environment Setup

**Crucial**: All tests run using the local dependency cache.
```bash
export PYTHONPATH="$(pwd)/libs:$PYTHONPATH"
```

## 2. Unit & Integration Tests (`pytest`)

We use `pytest` for code correctness and logic verification.

### Run All Tests
```bash
# Must set PYTHONPATH first!
export PYTHONPATH="$(pwd)/libs:$PYTHONPATH"
python3 -m pytest tests/ -v
```

### Core Test Suites
- `tests/test_feature_store.py`: **Critical**. Verifies Date-Based Temporal Logic and Point-in-Time correctness.
- `tests/test_strategic_engine.py`: Verifies high-level logic.

## 3. Data Integrity & Population Audits

Beyond code tests, we audit the **Data State**.

### Population Density Audit
Checks the "High Cap" vs "Low Cap" coverage balance.
```bash
# Run the audit script
python3 scripts/population_audit.py
```
**Pass Criteria**:
- High Cap (>$10M) Coverage > 99%
- Low Cap (<$2M) Coverage > 85%
- High Cap R2 Score > 0.80

### Feature Store Materialization Check
Verify the Feature Store is hydrated correctly.
```bash
python3 scripts/materialize_features.py
```
**Success**: "Completed successfully" log message with no temporal validity errors.

## 4. End-to-End Pipeline Verification

To simulate a full weekly run:
```bash
# 1. Update Silver Layer (if needed)
# ...

# 2. Materialize Gold Features
python3 scripts/materialize_features.py

# 3. Train Model & Verify R2
python3 src/train_model.py
```

## 5. Troubleshooting Common Issues

**"ModuleNotFoundError: No module named 'duckdb'"**
- **Fix**: You forgot `export PYTHONPATH="$(pwd)/libs:$PYTHONPATH"`.

**"Feature store returned empty dataframe"**
- **Fix**: The `feature_values` table is empty. Run `scripts/materialize_features.py`.

**"Permission Error: /var/folders/..."**
- **Fix**: This is a known macOS Python cache issue. Ignore it if execution proceeds, or run with `PYTHONDONTWRITEBYTECODE=1`:
  ```bash
  export PYTHONDONTWRITEBYTECODE=1
  python3 ...
  ```
