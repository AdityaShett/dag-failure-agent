"""
Seeds GCS outcome-history blobs and BigQuery fix_history rows so that
s_history and s_retrieval have real, non-default variance to tune against.

Without this, s_retrieval sits near 0 for every new task_id (nothing to
retrieve) and s_history sits at exactly 0.5 (the no-data default in
_fetch_history_score) — meaning two of your five signals can never
discriminate high vs low confidence no matter how good your scenarios are.

Run ONCE, before publish_batch_diverse.py. Running it twice will duplicate
seed rows/blobs.

Requires: GCP_PROJECT and OUTCOMES_BUCKET env vars set, same auth as the
rest of the pipeline (gcloud auth application-default login).
"""
import os
import json
import uuid
from datetime import datetime, timezone

from google.cloud import storage as gcs_storage
from google.cloud import bigquery

PROJECT = os.environ["GCP_PROJECT"]
OUTCOMES_BUCKET = os.environ["OUTCOMES_BUCKET"]
BQ_TABLE = f"{PROJECT}.dag_failure_agent.fix_history"

storage_client = gcs_storage.Client()
bucket = storage_client.bucket(OUTCOMES_BUCKET)

# --- 1. Seed GCS history/ blobs so _fetch_history_score has real signal ---
# (score = merged_count / len(blobs) for that exact dag_id+task_id pair)

EASY_TASKS = [
    ("dag1", "load_dataset_easy"),
    ("dag2", "merge_partitions_easy"),
    ("dag3", "normalize_records_easy"),
]
HARD_TASKS = [
    ("dag4", "enrich_with_scores_hard"),
    ("dag5", "write_report_to_gcs_hard"),
    ("dag6", "call_partner_api_hard"),
]


def seed_gcs_history(dag_id, task_id, merged_flags):
    for i, merged in enumerate(merged_flags):
        fake_pr = 90000 + i  # out-of-range fake PR numbers, won't collide with real ones
        record = {
            "dag_id": dag_id,
            "task_id": task_id,
            "run_id": f"seed-{uuid.uuid4().hex[:8]}",
            "merged": merged,
            "pr_number": fake_pr,
            "root_cause": "seed data for weight tuning",
            "proposed_fix": "seed data",
            "confidence_record_id": None,
        }
        blob = bucket.blob(f"history/{dag_id}-{task_id}/{fake_pr}.json")
        blob.upload_from_string(json.dumps(record))
    print(f"Seeded {len(merged_flags)} history blobs for {dag_id}/{task_id} (merged={merged_flags})")


for dag_id, task_id in EASY_TASKS:
    seed_gcs_history(dag_id, task_id, [True, True, True, True])   # -> s_history ~1.0

for dag_id, task_id in HARD_TASKS:
    seed_gcs_history(dag_id, task_id, [False, False, False, True])  # -> s_history ~0.25


# --- 2. Seed BigQuery fix_history rows (with real embeddings) so
#         retrieval has genuine merged-fix matches for the "easy" cases.
#         Deliberately NOT seeding anything for the "hard" tasks — no
#         precedent is the point. ---

from langchain_google_vertexai import VertexAIEmbeddings

embeddings = VertexAIEmbeddings(model_name="text-embedding-005")
bq_client = bigquery.Client(project=PROJECT)

EASY_SEED_FIXES = [
    {
        "dag_id": "dag1", "task_id": "load_dataset_easy",
        "root_cause": "KeyError: 'user_id' — the source column was renamed to 'uid' upstream but load_dataset still references the old name.",
        "proposed_fix": "--- a/tests/dag1.py\n+++ b/tests/dag1.py\n@@ -10,7 +10,7 @@\n-    df['user_id']\n+    df['uid']\n",
    },
    {
        "dag_id": "dag2", "task_id": "merge_partitions_easy",
        "root_cause": "FileNotFoundError — merge_partitions hardcodes a partition path using the wrong date-format macro.",
        "proposed_fix": "--- a/tests/dag2.py\n+++ b/tests/dag2.py\n@@ -14,7 +14,7 @@\n-    path = f'{base}/{ds}'\n+    path = f'{base}/{ds_nodash}'\n",
    },
    {
        "dag_id": "dag3", "task_id": "normalize_records_easy",
        "root_cause": "TypeError: NoneType has no attribute 'strip' — normalize_records doesn't guard against a null field before calling .strip().",
        "proposed_fix": "--- a/tests/dag3.py\n+++ b/tests/dag3.py\n@@ -8,7 +8,7 @@\n-    value.strip()\n+    (value or '').strip()\n",
    },
]

rows = []
for fix in EASY_SEED_FIXES:
    embed_text = f"Root cause:\n{fix['root_cause']}\n\nFix:\n{fix['proposed_fix']}"
    embedding = embeddings.embed_query(embed_text)
    rows.append({
        "record_id": str(uuid.uuid4()),
        "dag_id": fix["dag_id"], "task_id": fix["task_id"],
        "run_id": f"seed-{uuid.uuid4().hex[:8]}",
        "root_cause": fix["root_cause"], "proposed_fix": fix["proposed_fix"],
        "outcome": "merged",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "embedding": embedding,
    })

errors = bq_client.insert_rows_json(BQ_TABLE, rows)
if errors:
    print(f"WARNING: fix_history seed insert failed: {errors}")
else:
    print(f"Seeded {len(rows)} fix_history rows with real embeddings for the 'easy' scenarios.")

print("\nDone. Do not re-run this script — it will duplicate blobs/rows if run again.")
