from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks import notify_on_failure

default_args = {
    "owner": "dataops",
    "retries": 0,
    "on_failure_callback": notify_on_failure,
}


def read_file():
    with open("/tmp/customer_extract.csv") as f:
        return f.read()


with DAG(
    dag_id="dag2",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    PythonOperator(
        task_id="read_file",
        python_callable=read_file,
    )

# Agent RCA Test
# DAG: dag2
# Task: test_task_0
