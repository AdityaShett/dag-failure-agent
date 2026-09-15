from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def fetch_upstream_partition(**context):
    partition_path = "/tmp/partitions/2026-08-30.parquet"
    os.makedirs(os.path.dirname(partition_path), exist_ok=True)
    with open(partition_path, 'w') as f:
        f.write("sample data for validation") # Create a dummy file for subsequent tasks
    context["ti"].xcom_push(key="partition_path", value=partition_path)

def validate_schema(**context):
    original_path = context["ti"].xcom_pull(key="partition_path", task_ids="fetch_upstream_partition")
    validated_path = original_path.replace(".parquet", ".validated.parquet")
    # Simulate schema validation by copying the file content to the validated path
    with open(original_path, 'r') as infile:
        content = infile.read()
    with open(validated_path, 'w') as outfile:
        outfile.write(content)
    print(f"Schema validated and validated file created at {validated_path}")

def merge_partitions(**context):
    path = context["ti"].xcom_pull(key="partition_path", task_ids="fetch_upstream_partition")    validated_path = path.replace(".parquet", ".validated.parquet")
    with open(validated_path) as f:  # BUG: validate_schema hasn't run yet when this executes
        print(f"Merging {validated_path}")

def publish_report(**context):
    print("Publishing merged report")

with DAG("dag2", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
    t1 = PythonOperator(task_id="fetch_upstream_partition", python_callable=fetch_upstream_partition)
    t2 = PythonOperator(task_id="validate_schema", python_callable=validate_schema)
    t3 = PythonOperator(task_id="merge_partitions", python_callable=merge_partitions)
    t4 = PythonOperator(task_id="publish_report", python_callable=publish_report)
    # --- BUG (intentional): merge_partitions and validate_schema both depend only on
    # fetch_upstream_partition, so Airflow can run them in parallel — merge_partitions
    # sometimes wins the race and reads a .validated file that doesn't exist yet.
    t1 >> t2 >> t3
    t3 >> t4


# Agent RCA Test
# DAG: dag2
# Task: merge_partitions_easy
