from datetime import datetime
import sqlite3

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks import notify_on_failure

default_args = {
    "owner": "dataops",
    "retries": 0,
    "on_failure_callback": notify_on_failure,
}


def run_query():
    conn = sqlite3.connect(":memory:")

    conn.execute("""
        SELECT *
        FROM sales
    """)


with DAG(
    dag_id="dag4",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    PythonOperator(
        task_id="run_query",
        python_callable=run_query,
    )

# Agent RCA Test
# DAG: dag4
# Task: test_task_12
