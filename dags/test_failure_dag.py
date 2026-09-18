from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
}

with DAG(
    'test_failure_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    failing_task = BashOperator(
        task_id='fail_task',
        bash_command='exit 1',
    )
