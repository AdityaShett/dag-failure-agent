from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def call_partner_api(**context):
    import requests
    # --- BUG (intentional): partner's /reconcile endpoint legitimately takes ~20s
    # under normal load; this timeout was copied from a different, faster endpoint ---
    resp = requests.get("https://partner.example.com/reconcile", timeout=60)
    context["ti"].xcom_push(key="response", value=resp.json())

def parse_response(**context):
    print("Parsing partner response")

def store_result(**context):
    print("Storing reconciliation result")

with DAG("dag6", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
    # The task 'run_batch_job' was identified as missing.
    # A placeholder PythonOperator is added to make the task exist,
    # as its intended functionality is not specified in the root cause.
    run_batch_job = PythonOperator(
        task_id="run_batch_job",


# Agent RCA Test
# DAG: dag6
# Task: call_partner_api_hard
