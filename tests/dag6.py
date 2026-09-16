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


def _run_batch_job_placeholder():
    """Placeholder function for the missing 'run_batch_job' task."""
    print("Executing placeholder for 'run_batch_job'. The actual logic for this task is not defined in the provided code.")

with DAG("dag6", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
    # This task is added as a placeholder because the logs indicate it was attempted,


# Agent RCA Test
# DAG: dag6
# Task: call_partner_api_hard
