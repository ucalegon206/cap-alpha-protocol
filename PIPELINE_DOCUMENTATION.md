# NFL Dead Money Pipeline - Production Documentation

## Overview

The **NFL Dead Money Pipeline** is a data orchestration system that ingests, transforms, and analyzes NFL salary cap impact from dead money contracts. It runs on **Apache Airflow 3.x** with **CeleryExecutor** and **Redis**, simulating production infrastructure used at major tech companies (Netflix, Uber, Airbnb).

---

## Architecture

### Infrastructure Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestrator** | Apache Airflow 3.x | Workflow scheduling & task management |
| **Executor** | CeleryExecutor | Distributed task execution |
| **Message Broker** | Redis | Task queue between scheduler & workers |
| **Database** | SQLite | Metadata store (DAG runs, task instances) |
| **Transform Engine** | dbt (DuckDB) | Data modeling & mart generation |
| **Storage** | DuckDB | Analytical data warehouse |

### Key Features

- **Distributed Execution**: Tasks run in parallel across Celery workers
- **Retry Policy**: 2 retries with 5-minute delays for fault tolerance
- **Weekly Scheduling**: `@weekly` schedule for automated runs
- **Task Dependencies**: Enforced DAG ordering with clear data lineage
- **Data Quality**: Validation checks before & after transformations

---

## Pipeline DAG: `nfl_dead_money_pipeline`

### Task Execution Flow

```
[Snapshot Tasks] (Parallel)
    ├─ snapshot_spotrac_team_cap (BashOperator)
    ├─ snapshot_player_rankings_weekly (BashOperator)
    └─ backfill_player_rankings_2015_2024 (BashOperator)
           ↓
[Staging Layer]
    └─ stage_spotrac_raw_to_staging (PythonOperator)
           ↓
[Validation]
    └─ validate_staging_tables (PythonOperator)
           ↓
[Seed Reference Data]
    └─ dbt_seed_spotrac (BashOperator)
           ↓
[Transform - Staging]
    └─ dbt_run_staging (BashOperator)
           ↓
[Normalize]
    └─ normalize_staging_to_processed (PythonOperator)
           ↓
[Transform - Marts]
    └─ dbt_run_marts (BashOperator)
           ↓
[Quality Checks]
    ├─ validate_player_rankings_quality (BashOperator)
    └─ validate_data_quality (PythonOperator)
           ↓
[Final Processing]
    ├─ scrape_rosters (PythonOperator)
    └─ merge_dead_money (PythonOperator)
```

### Task Definitions

#### 1. **Snapshot Tasks** (Parallel Execution)
- **snapshot_spotrac_team_cap**: Downloads current season team cap data from Spotrac
- **snapshot_player_rankings_weekly**: Collects player ranking data weekly
- **backfill_player_rankings_2015_2024**: One-time historical load of player rankings

#### 2. **Staging Layer**
- **stage_spotrac_raw_to_staging**: Loads raw Spotrac CSV files into staging tables
  - Uses: `src/ingestion.py`
  - Input: Raw CSV files in `data/raw/`
  - Output: Staging tables in DuckDB

#### 3. **Validation**
- **validate_staging_tables**: Data quality checks
  - Null checks, uniqueness, referential integrity
  - Uses: `src/data_quality_tests.py`

#### 4. **Reference Data**
- **dbt_seed_spotrac**: Loads seed data for team mappings, historical constants
  - Location: `dbt/seeds/`

#### 5. **Transformation - Staging**
- **dbt_run_staging**: dbt models for `staging/*`
  - Cleaning, deduplication, type casting
  - Joins raw and seed data

#### 6. **Normalization**
- **normalize_staging_to_processed**: Custom Python transformations
  - Aggregations, calculated fields, business logic
  - Uses: `src/normalization.py`

#### 7. **Transformation - Marts**
- **dbt_run_marts**: dbt models for `marts/*`
  - Final aggregates: team dead money, player dead money, year-over-year trends
  - Optimized for reporting & dashboards

#### 8. **Quality Assurance**
- **validate_player_rankings_quality**: Season-specific validation (retries 3x)
- **validate_data_quality**: Full pipeline validation

#### 9. **Final Processing**
- **scrape_rosters**: Collect current NFL rosters from Pro Football Reference
- **merge_dead_money**: Enrich roster data with dead money impact

---

## Data Flow

### Input Sources
1. **Spotrac**: Team cap data (CSV snapshots)
2. **Pro Football Reference (PFR)**: Historical rosters, contracts
3. **Manual Upload**: Dead money CSV (OTC, NFLPA)

### Transformations
- **Raw → Staging**: Type conversion, normalization, deduplication
- **Staging → Processing**: Business logic, aggregations, enrichment
- **Processing → Marts**: Final models for analytics, reporting

### Output Tables (DuckDB)

| Table | Purpose | Updated |
|-------|---------|---------|
| `team_dead_money` | Annual dead money by team | Weekly |
| `player_dead_money` | Player-level dead money impact | Weekly |
| `roster_contracts` | Enriched roster + contract data | Weekly |
| `cap_trends` | Year-over-year cap trends | Weekly |

### Parquet Sidecars

- Location: `data/processed/compensation/parquet/{table}/year=YYYY/part-000.parquet`
- Tables emitted: `stg_team_cap`, `stg_player_rankings`, `stg_dead_money` (CSV remains for compatibility)
- Notes: Requires `pyarrow` or `fastparquet`; writes are skipped (with warning) if engine is missing.

---

## Configuration

### Environment Variables

```bash
# Airflow
AIRFLOW_HOME=/path/to/airflow
AIRFLOW__CORE__DAGS_FOLDER=/path/to/dags
AIRFLOW__CORE__EXECUTOR=CeleryExecutor

# Celery
AIRFLOW__CELERY__BROKER_URL=redis://localhost:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=redis://localhost:6379/0

# DAG Settings
AIRFLOW__CELERY__WORKER_SKIP_STALE_BUNDLE_CLEANUP=true
```

### Retry Policy

```python
DEFAULT_ARGS = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}
```

---

## Running the Pipeline

### Start Services

```bash
# Start Redis (required for Celery)
brew services start redis

# Start Scheduler (background)
AIRFLOW_HOME=$(pwd)/airflow \
  AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  AIRFLOW__CELERY__BROKER_URL="redis://localhost:6379/0" \
  AIRFLOW__CELERY__RESULT_BACKEND="redis://localhost:6379/0" \
  PYTHONPATH=$(pwd) \
  ./.venv/bin/airflow scheduler -l info &

# Start Celery Worker (background)
AIRFLOW_HOME=$(pwd)/airflow \
  AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  AIRFLOW__CELERY__BROKER_URL="redis://localhost:6379/0" \
  AIRFLOW__CELERY__RESULT_BACKEND="redis://localhost:6379/0" \
  AIRFLOW__CELERY__WORKER_SKIP_STALE_BUNDLE_CLEANUP=true \
  PYTHONPATH=$(pwd) \
  ./.venv/bin/airflow celery worker -l info \
    --without-gossip --without-mingle --without-heartbeat &
```

### Local Lineage/Metadata Quickstart (DataHub + OpenLineage)

1) Start DataHub locally (Docker quickstart):

```bash
pip install --upgrade datahub && datahub docker quickstart
# UI: http://localhost:9002
```

2) Configure OpenLineage env for Airflow/dbt runs:

```bash
export OPENLINEAGE_URL=http://localhost:5000
export OPENLINEAGE_NAMESPACE=nfl-dead-money
```

3) Point DataHub client to local GMS:

```bash
export DATAHUB_GMS_HOST=localhost
export DATAHUB_GMS_PORT=8080
```

4) After `dbt run`/`dbt test`, emit metadata (example):

```bash
datahub ingest -c datahub/lineage_openlineage_to_datahub.yml
```

5) Record key datasets for cataloging (DuckDB + Parquet):
- DuckDB tables: `mart_team_summary`, `mart_player_cap_impact`
- Parquet sidecars: `stg_team_cap`, `stg_player_rankings`, `stg_dead_money`

Add these env vars to Airflow connections/worker env so DAG tasks emit lineage automatically when OpenLineage hooks are added.

### Trigger a Run

```bash
AIRFLOW_HOME=$(pwd)/airflow \
  AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  AIRFLOW__CELERY__BROKER_URL="redis://localhost:6379/0" \
  PYTHONPATH=$(pwd) \
  ./.venv/bin/airflow dags trigger nfl_dead_money_pipeline
```

### Monitor Execution

```bash
# List active DAG runs
AIRFLOW_HOME=$(pwd)/airflow PYTHONPATH=$(pwd) \
  ./.venv/bin/python -c "
from airflow.models import TaskInstance
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///airflow/airflow.db')
Session = sessionmaker(bind=engine)
session = Session()
tasks = session.query(TaskInstance).filter(
    TaskInstance.dag_id == 'nfl_dead_money_pipeline'
).all()
for t in tasks:
    print(f'{t.task_id}: {t.state}')
"
```

---

## Testing

### Run dbt Tests

```bash
cd dbt
../.venv/bin/dbt test --project-dir . --profiles-dir .
```

### Run Data Quality Tests

```bash
PYTHONPATH=. ./.venv/bin/python scripts/e2e_test.py
```

---

## Production Considerations

### Scaling Strategies

1. **Multiple Workers**: Add more Celery workers for parallel execution
2. **Task Pooling**: Limit concurrent tasks with Airflow pools
3. **Result Backend**: Use PostgreSQL instead of Redis for result storage
4. **Monitoring**: Integrate with Datadog/New Relic for observability

### High Availability

- **Scheduler HA**: Use `KubernetesExecutor` for multi-node scheduling
- **Worker Nodes**: Deploy on Kubernetes or EC2 autoscaling groups
- **Database**: Use managed PostgreSQL (AWS RDS, Google Cloud SQL)
- **Message Broker**: Use managed Redis (AWS ElastiCache, Google Memorystore)

### Data Quality & Observability

- **Validation**: dbt tests + custom Python validators
- **Alerting**: Slack notifications on task failures
- **Logging**: Centralized logs (ELK stack, CloudWatch)
- **Monitoring**: Prometheus metrics on pipeline duration & success rates

---

## Troubleshooting

### Tasks Stuck in Queued State
- Check Redis connection: `redis-cli ping`
- Verify worker is running: `ps aux | grep celery`
- Check scheduler logs: `tail -f airflow/logs/scheduler/*.log`

### Stale Tasks
- Clear task instances: `AIRFLOW_HOME=$(pwd)/airflow ./.venv/bin/airflow db clean`
- Reset DAG serialization: Re-parse DAG with `dag-processor`

### Memory Issues
- Reduce `parallelism` setting in `airflow.cfg`
- Increase worker concurrency limits
- Monitor task resource usage

---

## Further Reading

- [Airflow Documentation](https://airflow.apache.org/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Celery Documentation](https://docs.celeryproject.io/)
- [DuckDB Documentation](https://duckdb.org/docs/)

---

**Last Updated**: December 20, 2025  
**Maintained By**: Data Engineering Team  
**Contact**: See README.md
