from agent.diff_utils import apply_unified_diff

dag_source = '''"""
dag-failure-agent - DEMO SCENARIO 1: HIGH CONFIDENCE

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
    "retries": 0,
    "on_failure_callback": notify_on_failure,
}

with DAG(
    dag_id="dag1",
    description="dag-failure-agent demo",
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
'''

proposed_fix_with_fences = '''```diff
--- a/dags/dag1.py
+++ b/dags/dag1.py
@@ -24,7 +24,7 @@

     # --- BUG (intentional): mistyped dictionary key ---
     # Should be config["dataset"]; this raises a KeyError every time.
-    dataset_name = config["datset"]
+    dataset_name = config["dataset"]

     print(f"Loading dataset: {dataset_name} ({config['format']}) from {config['region']}")

```'''

print("--- Attempt WITH markdown fences (what pr.py actually received) ---")
try:
    result = apply_unified_diff(dag_source, proposed_fix_with_fences)
    print("SUCCESS")
except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")

print("\n--- Attempt WITHOUT fences (manually stripped) ---")
stripped = proposed_fix_with_fences.strip()
if stripped.startswith("```"):
    stripped = stripped.split("\n", 1)[1]
if stripped.endswith("```"):
    stripped = stripped.rsplit("```", 1)[0]
stripped = stripped.strip() + "\n"

try:
    result = apply_unified_diff(dag_source, stripped)
    print("SUCCESS")
    print(result)
except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")
