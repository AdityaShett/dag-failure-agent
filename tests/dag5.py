from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# --- BUG (intentional): bucket belongs to a different project than this DAG's
# service account has storage.objectCreator on ---
REPORT_BUCKET = "gs://airflow-data/"

def extract_reporting_data(**context):
    context["ti"].xcom_push(key="report_path", value="/tmp/report_2026_08_30.csv")

def write_report_to_gcs(**context):
    from google.cloud import storage
    path = context["ti"].xcom_pull(key="report_path", task_ids="extract_reporting_data")
    client = storage.Client()
    bucket = client.bucket(REPORT_BUCKET.replace("gs://", "").rstrip("/"))
    blob = bucket.blob("reports/2026-08-30.csv")
    blob.upload_from_filename(path, timeout=60)  # raises 403 Forbidden

def archive_source(**context):
    print("Archiving source extract")

with DAG("dag5", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
    t1 = PythonOperator(task_id="extract_reporting_data", python_callable=extract_reporting_data)
    t2 = PythonOperator(task_id="write_report_to_gcs", python_callable=write_report_to_gcs)
    t3 = PythonOperator(task_id="archive_source", python_callable=archive_source)
    t1 >> t2 >> t3

# Agent RCA Test
# DAG: dag5
# Task: write_report_to_gcs
