from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def transform():

    amount = "abc"

    value = int(amount)

    return value

with DAG(
    dag_id="data_quality_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    PythonOperator(
        task_id="transform",
        python_callable=transform
    )