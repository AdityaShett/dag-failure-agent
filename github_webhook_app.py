import hashlib
import hmac
import json
import os
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import bigquery

app = FastAPI()

_sm_client = secretmanager.SecretManagerServiceClient()
_storage_client = storage.Client()
_bq_client = bigquery.Client()

_PROJECT = os.environ["GCP_PROJECT"]
_OUTCOMES_BUCKET = os.environ["OUTCOMES_BUCKET"]
_BQ_DATASET = os.environ.get("BQ_DATASET", "dag_failure_agent")
_BQ_TABLE = f"{_PROJECT}.{_BQ_DATASET}.fix_history"

_secret_path = f"projects/{_PROJECT}/secrets/github-webhook-secret/versions/latest"
_WEBHOOK_SECRET = _sm_client.access_secret_version(name=_secret_path).payload.data.decode("utf-8")


def _verify_signature(body: bytes, signature_header: str) -> bool:
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = hmac.new(_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature_header)


def _record_to_fix_history(record: dict, pr_number: int, merged: bool):
    root_cause = record.get("root_cause", "")
    proposed_fix = record.get("proposed_fix", "")

    # Nothing meaningful to embed/retrieve later — skip
    if not root_cause and not proposed_fix:
        print(f"No root_cause/proposed_fix in record for PR #{pr_number}, skipping BQ write")
        return

    try:
        from langchain_google_vertexai import VertexAIEmbeddings

        embeddings = VertexAIEmbeddings(model_name="text-embedding-005")
        embed_text = f"Root cause:\n{root_cause}\n\nFix:\n{proposed_fix}"
        embedding = embeddings.embed_query(embed_text)

        row = {
            "record_id": str(uuid.uuid4()),
            "dag_id": record.get("dag_id", ""),
            "task_id": record.get("task_id", ""),
            "run_id": record.get("run_id", ""),
            "root_cause": root_cause,
            "proposed_fix": proposed_fix,
            "outcome": "merged" if merged else "rejected",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "embedding": embedding,
        }

        errors = _bq_client.insert_rows_json(_BQ_TABLE, [row])
        if errors:
            print(f"WARNING: BigQuery insert failed for PR #{pr_number}: {errors}")
        else:
            print(f"Inserted fix_history row for PR #{pr_number} (outcome={row['outcome']})")

    except Exception as e:
        print(f"WARNING: failed to write fix_history row for PR #{pr_number}: {e!r}")


@app.post("/webhook")
async def webhook(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Hub-Signature-256", "")

    if not _verify_signature(body, signature):
        raise HTTPException(status_code=401, detail="Invalid signature")

    event = request.headers.get("X-GitHub-Event")
    payload = json.loads(body)

    if event != "pull_request":
        return {"status": "ignored", "event": event}

    action = payload.get("action")
    if action != "closed":
        return {"status": "ignored", "action": action}

    pr = payload["pull_request"]
    pr_number = pr["number"]
    merged = pr.get("merged", False)

    bucket = _storage_client.bucket(_OUTCOMES_BUCKET)
    pending_blob = bucket.blob(f"pending/{pr_number}.json")

    if not pending_blob.exists():
        print(f"No pending record for PR #{pr_number}, skipping")
        return {"status": "no_pending_record"}

    record = json.loads(pending_blob.download_as_text())
    record["merged"] = merged
    record["pr_number"] = pr_number

    dag_id = record["dag_id"]
    task_id = record["task_id"]
    history_blob = bucket.blob(f"history/{dag_id}-{task_id}/{pr_number}.json")
    history_blob.upload_from_string(json.dumps(record))
    pending_blob.delete()

    _record_to_fix_history(record, pr_number, merged)
    _update_confidence_outcome(record, pr_number, merged)

    print(f"Recorded outcome: PR #{pr_number} merged={merged} dag={dag_id} task={task_id}")

    return {"status": "recorded", "merged": merged}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


def _update_confidence_outcome(record: dict, pr_number: int, merged: bool):
    record_id = record.get("confidence_record_id")
    if not record_id:
        return
    row = {
        "record_id": record_id,
        "outcome": "merged" if merged else "rejected",
        "pr_number": pr_number,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    table = f"{_PROJECT}.dag_failure_agent.confidence_outcomes"
    try:
        errors = _bq_client.insert_rows_json(table, [row])
        if errors:
            print(f"WARNING: confidence_outcomes insert failed: {errors}")
    except Exception as e:
        print(f"WARNING: failed to record confidence outcome: {e!r}")