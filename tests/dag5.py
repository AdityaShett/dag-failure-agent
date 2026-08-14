from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


default_args = {
    "retries": 0
}

def transform():

    amount = "abc"

    value = int(amount)

    return value

with DAG(
    dag_id="dag5",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    PythonOperator(
        task_id="transform",
        python_callable=transform
    )