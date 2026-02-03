
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum
from datetime import timedelta

default_args = {
    'owner': 'nfl_analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nfl_fair_value_pipeline',
    default_args=default_args,
    description='Scrape NFL salary and performance data',
    schedule='@weekly',
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['nfl', 'scraping'],
) as dag:


    # Production Weekly Pipeline
    # 1. Scrape Current Season Logic (Incremental)
    # 2. Scrape Future Data (Contracts change)
    # 3. ETL Transform
    
    current_year = 2024
    future_years = [2025, 2026]
    
    # Task 1: Scrape PFR (Current Season - Force Update)
    scrape_pfr_current = BashOperator(
        task_id=f'scrape_pfr_{current_year}',
        bash_command=f'python /opt/airflow/dags/repo/src/run_historical_scrape.py --year {current_year} --source pfr --force'
    )
    
    # Task 2: Scrape Spotrac (Future - Force Update as contracts valid)
    # We loop or create parallel tasks
    scrape_spotrac_tasks = []
    for yr in future_years:
        t = BashOperator(
            task_id=f'scrape_spotrac_{yr}',
            bash_command=f'python /opt/airflow/dags/repo/src/run_historical_scrape.py --year {yr} --source spotrac --force'
        )
        scrape_spotrac_tasks.append(t)

    # Task 2b: Scrape Detailed Contract Structure (Current Year) [NEW]
    # This fetches Guarantees/Dead Cap Structure
    scrape_details = BashOperator(
        task_id=f'scrape_contract_details_{current_year}',
        bash_command=f'python /opt/airflow/dags/repo/src/run_contract_details_scrape.py --year {current_year}'
    )
        
    # Task 3: Run Canonical Timeline Build (Replaces legacy ETL)
    # Builds 'canonical_player_timeline.parquet'
    run_timeline = BashOperator(
        task_id='build_canonical_timeline',
        bash_command='python /opt/airflow/dags/repo/src/player_timeline.py'
    )
    
    # Task 4: Hyperscale Feature Matrix
    build_features = BashOperator(
        task_id='build_feature_matrix',
        bash_command='python /opt/airflow/dags/repo/src/feature_factory.py'
    )
    
    # Task 5: Production XGBoost Model
    train_model = BashOperator(
        task_id='train_production_model',
        bash_command='python /opt/airflow/dags/repo/src/train_model.py'
    )
    
    # Task 6: Reporting
    run_report = BashOperator(
        task_id='generate_weekly_report',
        bash_command='python /opt/airflow/dags/repo/src/run_historical_analysis.py'
    )

    # Dependencies
    # Scrapers run in parallel
    scrape_pfr_current >> run_timeline
    scrape_details >> run_timeline
    for t in scrape_spotrac_tasks:
        t >> run_timeline
        
    run_timeline >> build_features >> train_model >> run_report
