
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum
from datetime import timedelta
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================
PROJECT_ROOT = "/opt/airflow/dags/repo" # Standard Airflow mapped path
VENV_PYTHON = f"{PROJECT_ROOT}/.venv/bin/python"

default_args = {
    'owner': 'nfl_analytics',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nfl_pipeline',
    default_args=default_args,
    description='End-to-end NFL Pipeline: Scrape -> Ingest -> Feature Store -> Model -> Reporting',
    schedule='@weekly',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=['nfl', 'production', 'financial-lift'],
) as dag:

    # ========================================================================
    # 1. SCRAPING (Fresh Data)
    # ========================================================================
    # Runs the historical scraper for current and near-future years to get fresh contracts/stats
    # Parameters: year (default 2025), week (default None which implies current)
    scrape_sources = BashOperator(
        task_id='scrape_sources',
        bash_command=f'cd {PROJECT_ROOT} && {VENV_PYTHON} src/run_historical_scrape.py --years {{{{ dag_run.conf.get("year", 2025) }}}} --week {{{{ dag_run.conf.get("week", "") }}}} --source all --force',
        pool='external_api',
        pool_slots=1,
    )

    # ========================================================================
    # 2. INGESTION & TRANSFORMATION (Raw -> Silver -> Gold)
    # ========================================================================
    # This script handles:
    # - Loading raw CSVs (Contracts, Financials, Merch, PFR Logs)
    # - Robust incremental load: Scans data/raw/{year}/* for latest files
    # - Creating Silver Tables
    # - Creating the Gold Layer (fact_player_efficiency) with Financial Lift logic
    ingest_and_transform = BashOperator(
        task_id='ingest_and_transform',
        bash_command=f'cd {PROJECT_ROOT} && {VENV_PYTHON} scripts/medallion_pipeline.py --year {{{{ dag_run.conf.get("year", 2025) }}}}',
    )

    # ========================================================================
    # 3. FEATURE ENGINEERING
    # ========================================================================
    # Generates Hyperscale Feature Matrix from Gold Layer
    build_features = BashOperator(
        task_id='build_feature_matrix',
        bash_command=f'cd {PROJECT_ROOT} && {VENV_PYTHON} src/feature_factory.py',
    )

    # ========================================================================
    # 4. MODEL TRAINING
    # ========================================================================
    # Trains XGBoost Risk Model on the new Feature Matrix
    train_model = BashOperator(
        task_id='train_production_model',
        bash_command=f'cd {PROJECT_ROOT} && {VENV_PYTHON} src/train_model.py',
    )

    # ========================================================================
    # 5. REPORTING
    # ========================================================================
    # Generates Financial Lift / ROI Report
    generate_report = BashOperator(
        task_id='generate_financial_report',
        bash_command=f'cd {PROJECT_ROOT} && {VENV_PYTHON} scripts/financial_lift_report.py',
    )

    # ========================================================================
    # DEPENDENCIES
    # ========================================================================
    scrape_sources >> ingest_and_transform >> build_features >> train_model >> generate_report
