# Data Pipeline Documentation

## Architectural Overview

The **Cap Alpha Protocol Pipeline** is a hermetic data engineering system designed to ingest, normalize, and feature-engineer NFL contract and performance data. It adheres to a **Medallion Architecture** patterns to ensure data quality and traceability.

### Directory Structure

| Directory | Purpose |
| :--- | :--- |
| `src/` | Core ETL logic, scrapers, and transformation functions. |
| `dags/` | Apache Airflow DAG definitions for orchestration. |
| `scripts/` | Standalone utility scripts for backfilling, auditing, and maintenance. |
| `contracts/` | Data contracts and schema definitions (Pydantic/Great Expectations). |

---

## Environment & Setup

We strictly enforce a **Local Library Strategy** to mitigate environment fragmentation on macOS Apple Silicon.

### Prerequisites
- Python 3.10+
- `pip`
- DuckDB

### Installation

```bash
# 1. Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install Dependencies
pip install -r requirements.txt

# 3. Set Python Path
export PYTHONPATH="$(pwd):$PYTHONPATH"
```

---

## Key Workflows

### 1. Data Ingestion (Scraping)
To ingest raw data from Spotrac or Pro Football Reference:

```bash
# Scrape Contract Data (e.g., 2024 season)
python3 scripts/ingest_contracts.py --year 2024

# Scrape Performance Data
python3 scripts/ingest_performance.py --year 2024
```

### 2. Feature Materialization
To populate the Feature Store (`feature_values` table) with point-in-time correct data:

```bash
python3 src/materialize_features.py
```

### 3. Training the Risk Engine
To train the XGBoost model on the materialized features:

```bash
python3 src/train_model.py
```

---

## Testing & Validation

We employ a rigorous testing suite to ensure data integrity.

```bash
# Run Unit Tests
pytest tests/unit

# Run Integration Tests
pytest tests/integration

# Population Audit
python3 scripts/population_audit.py
```
