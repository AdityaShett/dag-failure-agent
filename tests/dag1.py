from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

CONFIG = {"dataset": "customer_events", "format": "parquet", "region": "us-central1"}

def extract_source_data(**context):
    context["ti"].xcom_push(key="raw_path", value="gs://raw/customer_events/2026-08-30/")

def load_dataset(**context):
    # --- BUG (intentional): mistyped dictionary key ---
    dataset_name = CONFIG["datset"]  # should be CONFIG["dataset"]
    print(f"Loading dataset: {dataset_name} ({CONFIG['format']}) from {CONFIG['region']}")

def transform_dataset(**context):
    print("Applying schema normalization")

def aggregate_regions(**context):
    print("Aggregating by region")

with DAG("dag1", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
    t1 = PythonOperator(task_id="extract_source_data", python_callable=extract_source_data)
    t2 = PythonOperator(task_id="load_dataset", python_callable=load_dataset)
    t3 = PythonOperator(task_id="transform_dataset", python_callable=transform_dataset)
    t4 = PythonOperator(task_id="aggregate_regions", python_callable=aggregate_regions)
    t1 >> t2 >> t3 >> t4