"""
DAG 3: Data Quality / Null Handling
Failure mode: AttributeError from calling a string method on a None value
that was not filtered out upstream.
Expected agent outcome: PR_CREATED (medium confidence fix).
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def clean_records(**context):
    records = [
        {"name": "Alice", "email": "alice@example.com"},
        {"name": "Bob", "email": None},
        {"name": "Cara", "email": "cara@example.com"},
    ]
    normalized = []
    for record in records:
        normalized_email = record["email"].lower() if record["email"] is not None else ""
        normalized.append(normalized_email)
    print(f"Normalized {len(normalized)} emails")

with DAG(
    dag_id="dag3",
    description="Data Quality / Null Handling",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["benchmark", "null-handling"],
) as dag:
    clean = PythonOperator(task_id="clean_records", python_callable=clean_records)
