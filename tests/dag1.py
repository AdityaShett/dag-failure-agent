from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from callbacks import notify_on_failure


default_args = {
    "owner": "dataops",
    "retries": 1,
    "on_failure_callback": notify_on_failure,
}


def extract_data():
    print("Extracting data...")


def transform_data():
    print("Transforming data...")

    records = [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
        {"id": 3, "value": 30},
    ]

    total = sum(r["value"] for r in records)
    print(f"Total value: {total}")


def load_data():
    print("Loading data...")

    config = {
        "target_table": "sales_daily",
        "mode": "append",
    }

    # DELIBERATE BUG
    # Key does not exist and will raise KeyError
    destination = config["dataset"]

    print(f"Loading into {destination}")


with DAG(
    dag_id="dag1",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rca-test"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    validation_task = BashOperator(
        task_id="validation",
        bash_command="echo Validation complete",
    )

    extract_task >> transform_task >> load_task >> validation_task


# Agent RCA Test
# DAG: dag1
# Task: load_data
