from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def read_file():
    with open("/tmp/customer_extract.csv") as f:
        return f.read()

with DAG(
    dag_id="missing_file_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    PythonOperator(
        task_id="read_file",
        python_callable=read_file
    )