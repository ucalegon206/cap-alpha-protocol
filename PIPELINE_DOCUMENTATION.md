# Pipeline Documentation & Runbook

**Operational Guide for the NFL Dead Money Pipeline**

## 1. Environment & Dependencies
**CRITICAL**: This project enforces a "Local Libs" strategy to bypass macOS restrictions.
Always load the local library path before execution:
```bash
export PYTHONPATH="$(pwd)/libs:$PYTHONPATH"
```

## 2. Core Workflows

### Phase A: Data Ingestion (Bronze -> Silver)
*(Legacy scrapers - run only if new data is needed)*
```bash
python3 src/scraper.py --year 2025
python3 src/enrich_contracts.py
```

### Phase B: Feature Materialization (Silver -> Gold)
**Frequency**: Whenever Silver data changes or new features are defined.
**Script**: `scripts/materialize_features.py`
**Actions**:
1.  Validates Temporal Integrity (dates).
2.  Calculates Lag Features (Date-Based).
3.  Calculates Interaction Features.
4.  Populates `feature_values` table.

```bash
python3 scripts/materialize_features.py
```

### Phase C: Model Training (Gold -> Predictions)
**Frequency**: Weekly (during season) or Ad-hoc.
**Script**: `src/train_model.py`
**Actions**:
1.  Retrieves `min_year=2015` to `max_year=2025` data from Feature Store.
2.  Performs Diagonal Join (As-Of Sept 1st).
3.  Trains XGBoost with Walk-Forward Validation.
4.  Generates SHAP plots and saves `prediction_results`.

```bash
python3 src/train_model.py
```

### Phase D: Audit & Monitoring
**Frequency**: Post-Training.
**Script**: `scripts/population_audit.py`
**Actions**:
1.  Checks Population Coverage (% of targets with features).
2.  Micro-Audits "Low Cap" vs "High Cap" performance (R2).

```bash
python3 scripts/population_audit.py
```

## 3. Configuration
Control model parameters and file paths via:
- `config/ml_config.yaml`
- `src/config_loader.py`

## 4. Troubleshooting
**"No module named duckdb"**:
- Ensure `PYTHONPATH` is set correctly.
- Ensure dependencies are installed in `./libs`.

**"Feature store returned empty dataframe"**:
- Run `scripts/materialize_features.py` to hydrate the store.
