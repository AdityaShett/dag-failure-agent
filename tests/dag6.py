from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

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


# Agent RCA Test
# DAG: dag6
# Task: call_partner_api_hard
