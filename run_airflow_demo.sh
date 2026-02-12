#!/bin/bash
# Local Airflow Demo Runner
# Usage: ./run_airflow_demo.sh

# 1. Initialize Airflow DB (SQLite)
export AIRFLOW_HOME=$(pwd)/airflow
mkdir -p $AIRFLOW_HOME/dags
cp pipeline/dags/pipeline.py $AIRFLOW_HOME/dags/

echo "Initializing Airflow Database..."
airflow db init

# 2. Create User
airflow users create \
    --username admin \
    --firstname Portfolio \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password admin \
    2>/dev/null || echo "User already exists"

# 3. Unpause DAG
echo "Unpausing Pipeline..."
airflow dags unpause pipeline

# 4. Trigger DAG
echo "Triggering Pipeline..."
airflow dags trigger pipeline

# 5. Show Status
echo "Pipeline Triggered! Run 'airflow standalone' to view UI at localhost:8080"
