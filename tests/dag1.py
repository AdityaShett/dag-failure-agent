"""
dag-failure-agent — DEMO SCENARIO 1: HIGH CONFIDENCE

This variant is designed to make the agent land in the HIGH confidence tier
(>= 0.75) and open a normal (non-prefixed) draft PR.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks import notify_on_failure


def load_dataset(**context):
    config = {
        "dataset": "customer_events",
        "region": "us-central1",
        "format": "parquet",
    }

    # --- BUG (intentional): mistyped dictionary key ---
    # Should be config["dataset"]; this raises a KeyError every time.
    dataset_name = config["datset"]

    print(f"Loading dataset: {dataset_name} ({config['format']}) from {config['region']}")


default_args = {
    "owner": "dag-failure-agent-demo",
    "retries": 0,  # terminal failure on first attempt — no retry-cycling in the UI
    "on_failure_callback": notify_on_failure,
}

with DAG(
    dag_id="dag1",
    description="dag-failure-agent demo — high-confidence scenario (KeyError typo)",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dag-failure-agent-demo", "high-confidence"],
) as dag:
    test_task = PythonOperator(
        task_id="test_task",
        python_callable=load_dataset,
    )


# Agent RCA Test
# DAG: dag1
# Task: test_task


# Agent RCA Test
# DAG: dag1
# Task: test_task
