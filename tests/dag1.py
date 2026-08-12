from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from callbacks import notify_on_failure

default_args = {
    "on_failure_callback": notify_on_failure,
    "retries": 0,
}

with DAG(
    dag_id="dag1",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["rca-test"],
) as dag:

    fail_task = BashOperator(
        task_id="deliberately_fail",
        bash_command="exit 1"
    )