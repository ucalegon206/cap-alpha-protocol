# Operations Runbook: Cap Alpha Protocol
**Status:** Production (Belichick Standard)
**Orchestrator:** Python (Custom DAG) + Makefile
**Database:** DuckDB (`data/nfl_belichick.db`)

---

## 1. The "Big Red Button" (Full Regeneration)
If you want to refresh the entire universe (Ingest -> Train -> Report), run this.
It mimics an Airflow DAG by executing steps sequentially and halting on error.

```bash
# Runs everything: Ingest (2011-2025) -> Validation -> Features -> Training -> Audit -> Charts
python run_pipeline.py
```

### Fast Mode (Iterating on Logic)
If you already have data and just want to retrain the model or fix a report:
```bash
# Skip the heavy lifting (Ingest/Feature Engineering)
python run_pipeline.py --skip-ingest --skip-features
```

---

## 2. Next Season Protocol (Manual Ingestion)
**Scenario:** It is Week 1 of the 2026 Season. You have new Spotrac files.

### Step A: Place the Files
Put the raw CSVs in the folder structure (Idempotent sensing):
`data/raw/2026/week_1/`

### Step B: Trigger Ingestion
You do **NOT** need to edit `run_pipeline.py` loops. You can target specific ingestion manually:

```bash
# Ingests 2026 data, updates Gold Layer, but stops before Training
python scripts/ingest_to_duckdb.py --year 2026 --week 1
```

### Step C: Train & Verify
```bash
# Retrain the XGBoost model with the new 2026 data included
python src/train_model.py
```

---

## 3. Architecture FAQ ("Is this a mess?")
**Q: Is this Airflow?**
A: No. It is a **Pattern-Based Python DAG**.
*   **Why?** Airflow is overhead for a single-tenant DuckDB file.
*   **Equivalent:** `run_pipeline.py` *is* your DAG file. `scripts/` are your Operators.
*   **Idempotency:** Yes. All scripts use `CREATE OR REPLACE` or `DELETE WHERE year=?` before inserting. You can run them 100 times without duplicating data.

**Q: DB Locks?**
A: We moved to `nfl_belichick.db` to bypass OS locks. Code automatically closes connections.

## 4. Visuals & Reporting
To regenerate the "Money Charts" (Valuation Matrix) without running the full pipeline:
```bash
make charts
# OR manual override
python scripts/generate_brand_value_chart.py
```
