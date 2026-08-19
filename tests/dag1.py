"""
dag-failure-agent — DEMO SCENARIO 1: HIGH CONFIDENCE

Purpose
-------
This variant is designed to make the agent land in the HIGH confidence tier
(>= 0.75) and open a normal (non-prefixed) draft PR.

Why this should score high, signal by signal:
  S_llm        -> The bug is a single, unambiguous typo in a dict key
                  ("datset" vs "dataset"). There is exactly one sensible fix,
                  so Gemini's self-reported confidence should be very high.
  S_retrieval  -> dag_id/task_id ("dag1" / "test_task") match the pair that
                  already has a real merged fix on file in the outcomes
                  bucket from earlier project testing, so the Chroma
                  knowledge base should return a directly relevant hit.
  S_history    -> Same reason as above: this exact (dag_id, task_id) pair
                  has a prior merged: true record, so S_history should
                  compute close to 1.0 instead of the neutral 0.5.
  S_logs       -> A real Composer task failure produces real log lines
                  (the traceback), so this signal gets meaningful length.
  S_source     -> The file is fetched successfully from GitHub, so this is
                  1.0 as long as the repo push in the guide was done first.

DEPLOYMENT NOTE (see TESTING-GUIDE.md):
  This file's *contents* get copied into tests/dag1.py before the demo run
  (both committed to GitHub and uploaded to the Composer DAGs bucket).
  Do not deploy this file under its own name — dag_id must be "dag1" to
  pass the receiver's ALLOWED_DAG_IDS filter.
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
