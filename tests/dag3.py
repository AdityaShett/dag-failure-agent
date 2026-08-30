from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks import notify_on_failure

default_args = {
    "owner": "dataops",
    "retries": 0,
    "on_failure_callback": notify_on_failure,
}


def process_api_data():
    response = {
        "status": "ok"
    }

    customer_id = response["customer"]["id"]
    return customer_id


with DAG(
    dag_id="dag3",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    PythonOperator(
        task_id="process_api_data",
        python_callable=process_api_data,
    )

# Agent RCA Test
# DAG: dag3
# Task: test_task_13
