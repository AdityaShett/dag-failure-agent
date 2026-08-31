from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def call_partner_api(**context):
    import requests
    # --- BUG (intentional): partner's /reconcile endpoint legitimately takes ~20s
    # under normal load; this timeout was copied from a different, faster endpoint ---
    resp = requests.get("https://partner.example.com/reconcile", timeout=30)
    context["ti"].xcom_push(key="response", value=resp.json())

def parse_response(**context):
    print("Parsing partner response")

def store_result(**context):
    print("Storing reconciliation result")

with DAG("dag6", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
    t1 = PythonOperator(task_id="call_partner_api", python_callable=call_partner_api)
    t2 = PythonOperator(task_id="parse_response", python_callable=parse_response)
    t3 = PythonOperator(task_id="store_result", python_callable=store_result)
    t1 >> t2 >> t3