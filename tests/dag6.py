"""
DAG 6: Truncated / Misconfigured Logging
Failure mode: the task fails but its logging is misconfigured, so the
worker log contains no exception detail or stack trace to act on.
Expected agent outcome: NO_CONFIDENT_FIX (negative test).
"""
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Misconfiguration: this logger is set to CRITICAL, so the exception
# logged by Airflow's task handler at ERROR level never reaches the log
# file, leaving no stack trace for the agent to analyze.
logging.getLogger("airflow.task").setLevel(logging.CRITICAL)


def run_batch_job(**context):
    raise RuntimeError("Batch job failed during step 3 of 7")


with DAG(
    dag_id="dag6",
    description="Truncated / Misconfigured Logging",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["benchmark", "ambiguous-log", "negative"],
) as dag:
    run_task = PythonOperator(task_id="run_batch_job", python_callable=run_batch_job)
