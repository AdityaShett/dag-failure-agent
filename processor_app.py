import base64
import json
import os

from fastapi import FastAPI, Request

from agent.graph import app as agent_graph

from google.cloud import storage

_storage_client = storage.Client()
_DEDUP_BUCKET = os.environ.get("DEDUP_BUCKET")

def _already_processed(run_id: str, task_id: str) -> bool:
    if not _DEDUP_BUCKET:
        return False
    bucket = _storage_client.bucket(_DEDUP_BUCKET)
    blob = bucket.blob(f"processed/{run_id}-{task_id}")
    if blob.exists():
        return True
    blob.upload_from_string("1")
    return False

app = FastAPI()


@app.post("/process")
async def process(request: Request):
    envelope = await request.json()
    pubsub_message = envelope["message"]

    raw_data = base64.b64decode(pubsub_message["data"]).decode("utf-8")
    payload = json.loads(raw_data)

    run_id = payload.get("run_id")
    task_id = payload.get("task_id")

    if _already_processed(run_id, task_id):
        print(f"Skipping duplicate: run_id={run_id} task_id={task_id}")
        return {"status": "duplicate_skipped"}

    dag_id = payload.get("dag_id")
    github_repo = payload.get("github_repo")

    # Use target_file from payload if provided (e.g. synthetic/manual test
    # runs), otherwise fall back to the automatic DAG -> file mapping.
    target_file = payload.get("target_file") or f"tests/{dag_id}.py"

    print(f"Processing: DAG={dag_id} REPO={github_repo} FILE={target_file}")

    result = agent_graph.invoke(
        {
            "dag_id": dag_id,
            "task_id": payload.get("task_id"),
            "run_id": payload.get("run_id"),
            "try_number": payload.get("try_number", 1),
            "github_repo": github_repo,
            "target_file": target_file,
            "synthetic_task_logs": payload.get("synthetic_task_logs"),
        }
    )

    print(f"Graph result: {result}")
    return {"status": "processed", "pr_url": result.get("pr_url")}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}