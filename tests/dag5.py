"""
DAG 5: API / Network Timeout (External)

"""
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


def call_partner_api(**context):
    response = requests.get(
        "https://partner-api.example.com/v2/report", timeout=30
    )
    response.raise_for_status()
    return response.json()


with DAG(
    dag_id="dag5",
    description="API / Network Timeout (External)",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["benchmark", "external-outage", "negative"],
) as dag:
    call_task = PythonOperator(
        task_id="call_partner_api", python_callable=call_partner_api
    )
