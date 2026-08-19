"""
dag-failure-agent — DEMO SCENARIO 2: MEDIUM CONFIDENCE

Purpose
-------
This variant is designed to make the agent land in the MEDIUM confidence
tier (0.50-0.74) and open a draft PR with a "[low-confidence]" title
prefix and the confidence score shown in the PR body.

Why this should score medium, signal by signal:
  S_llm        -> The bug is a real but genuinely ambiguous data-quality
                  issue (a malformed upstream record missing a nested
                  field). There isn't one obviously "correct" fix — skip
                  the bad record, default the missing field, or push
                  validation further upstream are all defensible — so
                  Gemini's self-reported confidence should land lower
                  than the single-typo scenario, roughly 0.5-0.7.
  S_retrieval  -> task_id "aggregate_regions" has never been seen before,
                  so the Chroma knowledge base (currently seeded mostly
                  with the test_task fix) should return few or no closely
                  relevant hits.
  S_history    -> No prior merged/rejected record exists yet for
                  (dag1, aggregate_regions), so this falls back to the
                  neutral 0.5 rather than a confirmed 1.0.
  S_logs       -> Same as scenario 1 — a real Composer failure, real logs.
  S_source     -> 1.0, same reasoning as scenario 1.

Note: LLM self-reported confidence isn't perfectly deterministic between
runs. This is designed to land in medium, not guaranteed to. See the
"If it doesn't land where expected" section of TESTING-GUIDE.md.

DEPLOYMENT NOTE (see TESTING-GUIDE.md):
  This file's *contents* get copied into tests/dag1.py before the demo run
  (both committed to GitHub and uploaded to the Composer DAGs bucket).
  Do not deploy this file under its own name — dag_id must be "dag1" to
  pass the receiver's ALLOWED_DAG_IDS filter. task_id is different from
  scenario 1 on purpose (see S_history/S_retrieval notes above).
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


# Agent RCA Test
# DAG: dag1
# Task: aggregate_regions
