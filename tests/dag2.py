"""
DAG 2: Partition / Macro Syntax

"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def read_partition(ds, **context):

    partition_path = f"/data/warehouse/events/dt={ds}/part-00000.csv"
    if not os.path.exists(partition_path):
        raise FileNotFoundError(
            f"No such file or directory: '{partition_path}'"
        )
    with open(partition_path, "r") as f:
        return f.read()


with DAG(
    dag_id="dag2",
    description="Partition / Macro Syntax",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["benchmark", "macro-mismatch"],
) as dag:
    read_task = PythonOperator(
        task_id="read_partition",
        python_callable=read_partition,
        op_kwargs={"ds": "{{ ds }}"},
    )
