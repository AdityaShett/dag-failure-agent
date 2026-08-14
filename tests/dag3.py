from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "retries": 0
}

def process_api_data():

    response = {
        "status": "ok"
    }

    customer_id = response["customer"]["id"]

    return customer_id

with DAG(
    dag_id="dag3",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    PythonOperator(
        task_id="process_api_data",
        python_callable=process_api_data
    )