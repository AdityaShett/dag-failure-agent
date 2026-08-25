"""
dag-failure-agent — DEMO SCENARIO 2: MEDIUM CONFIDENCE
This variant is designed to make the agent land in the MEDIUM confidence
tier (0.50-0.74) and open a draft PR with a "[low-confidence]" title
prefix and the confidence score shown in the PR body.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks import notify_on_failure


def aggregate_regions(**context):
    # Simulated records from an upstream source. One record is malformed —
    # it's missing the "metadata" field entirely, which a real upstream
    # producer occasionally does for events created before a schema change.
    records = [
        {"customer_id": "C-1001", "metadata": {"region": "us-east1"}},
        {"customer_id": "C-1002", "metadata": {"region": "us-west1"}},
        {"customer_id": "C-1003"},  # <-- BUG (intentional): no "metadata" key
        {"customer_id": "C-1004", "metadata": {"region": "eu-west1"}},
    ]

    region_counts: dict[str, int] = {}
    for record in records:
        # --- BUG (intentional): record.get("metadata") is None for C-1003,
        # so .get("region") on the next line raises AttributeError.
        # There is no single obviously-correct fix here — that's deliberate.
        region = record.get("metadata").get("region")
        region_counts[region] = region_counts.get(region, 0) + 1

    print(f"Region counts: {region_counts}")


default_args = {
    "owner": "dag-failure-agent-demo",
    "retries": 0,  # terminal failure on first attempt — no retry-cycling in the UI
    "on_failure_callback": notify_on_failure,
}

with DAG(
    dag_id="dag1",
    description="dag-failure-agent demo — medium-confidence scenario (ambiguous null field)",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dag-failure-agent-demo", "medium-confidence"],
) as dag:
    aggregate_task = PythonOperator(
        task_id="aggregate_regions",
        python_callable=aggregate_regions,
    )
