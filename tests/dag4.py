from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3

def run_query():

    conn = sqlite3.connect(":memory:")

    conn.execute("""
        SELECT *
        FROM sales
    """)

with DAG(
    dag_id="sql_failure_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    PythonOperator(
        task_id="run_query",
        python_callable=run_query
    )