"""
DAG 1: Schema / Column Drift
Failure mode: KeyError caused by an upstream schema change (a column was
renamed before this task's transform logic runs).
Expected agent outcome: PR_CREATED (easy / high-confidence fix).
"""
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator


def load_dataset(**context):
    # Upstream extract step. In production this reads from a warehouse
    # table; here it simulates the schema drift: the source system
    # renamed "user_id" -> "uid" but this DAG was not updated.
    data = {
        "uid": [101, 102, 103],
        "event_type": ["click", "view", "purchase"],
        "amount": [0.0, 0.0, 49.99],
    }
    df = pd.DataFrame(data)
    context["ti"].xcom_push(key="raw_df", value=df.to_dict())


def transform_data(**context):
    raw = context["ti"].xcom_pull(key="raw_df", task_ids="load_dataset")
    df = pd.DataFrame(raw)
    user_ids = df["uid"].tolist()
    print(f"Processed {len(user_ids)} user records")

with DAG(
    dag_id="dag1",
    description="Schema / Column Drift",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["benchmark", "schema-drift"],
) as dag:
    load = PythonOperator(task_id="load_dataset", python_callable=load_dataset)
    transform = PythonOperator(task_id="transform_data", python_callable=transform_data)
    load >> transform
