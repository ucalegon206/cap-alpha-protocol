
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
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
    schedule_interval='@weekly',
    start_date=days_ago(1),
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
        
    # Task 3: Run ETL (All Years)
    run_etl = BashOperator(
        task_id='run_etl_transform',
        bash_command='python /opt/airflow/dags/repo/src/etl_transform.py'
    )
    
    # Task 4: Reporting
    run_report = BashOperator(
        task_id='generate_weekly_report',
        bash_command='python /opt/airflow/dags/repo/src/run_historical_analysis.py'
    )

    # Dependencies
    # Scrapers run in parallel
    scrape_pfr_current >> run_etl
    for t in scrape_spotrac_tasks:
        t >> run_etl
        
    run_etl >> run_report
