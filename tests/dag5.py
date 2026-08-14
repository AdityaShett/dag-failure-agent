from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks import notify_on_failure

default_args = {
    "owner": "dataops",
    "retries": 0,
    "on_failure_callback": notify_on_failure,
}


def transform():
    amount = "abc"
    value = int(amount)
    return value


with DAG(
    dag_id="dag5",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    PythonOperator(
        task_id="transform",
        python_callable=transform,
    )