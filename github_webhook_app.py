import hashlib
import hmac
import json
import os

from fastapi import FastAPI, Request, HTTPException
from google.cloud import secretmanager
from google.cloud import storage

app = FastAPI()

_sm_client = secretmanager.SecretManagerServiceClient()
_storage_client = storage.Client()

_PROJECT = os.environ["GCP_PROJECT"]
_OUTCOMES_BUCKET = os.environ["OUTCOMES_BUCKET"]

_secret_path = f"projects/{_PROJECT}/secrets/github-webhook-secret/versions/latest"
_WEBHOOK_SECRET = _sm_client.access_secret_version(name=_secret_path).payload.data.decode("utf-8")


def _verify_signature(body: bytes, signature_header: str) -> bool:
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = hmac.new(_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature_header)


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

    print(f"Recorded outcome: PR #{pr_number} merged={merged} dag={dag_id} task={task_id}")

    return {"status": "recorded", "merged": merged}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}