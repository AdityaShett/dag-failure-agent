from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def fetch_upstream_partition(**context):
    context["ti"].xcom_push(key="partition_path", value="/tmp/partitions/2026-08-30.parquet")

def validate_schema(**context):
    path = context["ti"].xcom_pull(key="partition_path", task_ids="fetch_upstream_partition")
    with open(path) as f:  # writes the validated copy merge_partitions depends on
        pass
    print(f"Schema validated for {path}")

def merge_partitions(**context):
    path = context["ti"].xcom_pull(key="partition_path", task_ids="fetch_upstream_partition")
    validated_path = path.replace(".parquet", ".validated.parquet")
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


# Agent RCA Test
# DAG: dag2
# Task: merge_partitions_easy
