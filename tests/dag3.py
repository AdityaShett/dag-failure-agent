from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract_api_data(**context):
    context["ti"].xcom_push(key="records", value=[{"id": 1, "ts": "2026-08-30T00:00:00Z"}])

def normalize_records(**context):
    # --- BUG (intentional): module was renamed from date_helpers to datetime_helpers
    # in a refactor; this import was never updated ---
    from utils.date_helpers import parse_ts
    records = context["ti"].xcom_pull(key="records", task_ids="extract_api_data")
    for r in records:
        r["ts"] = parse_ts(r["ts"])
    context["ti"].xcom_push(key="normalized", value=records)

def load_to_warehouse(**context):
    print("Loading normalized records to warehouse")

with DAG("dag3", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
    t1 = PythonOperator(task_id="extract_api_data", python_callable=extract_api_data)
    t2 = PythonOperator(task_id="normalize_records", python_callable=normalize_records)
    t3 = PythonOperator(task_id="load_to_warehouse", python_callable=load_to_warehouse)
    t1 >> t2 >> t3