from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from load_data_python import main

with DAG(
    dag_id="load_raw_data_and_transform",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    description="Atomically refresh synthetic pet sources, then build and test reporting models.",
    tags=["portfolio", "pets", "elt"],
    dagrun_timeout=timedelta(minutes=20),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=8),
    },
) as dag:

    load_raw_data = PythonOperator(
        task_id="load_raw_data",
        python_callable=main,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="dbt build --fail-fast",
        cwd="/opt/airflow/dbt",
    )

    load_raw_data >> dbt_build
