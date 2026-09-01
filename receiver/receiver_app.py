import base64
import json
import os
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from google.cloud import pubsub_v1

app = FastAPI()

publisher = pubsub_v1.PublisherClient()
processing_topic_path = publisher.topic_path(
    os.environ["GCP_PROJECT"], "dagfailures-processing"
)

ALLOWED_DAG_IDS = os.environ.get("ALLOWED_DAG_IDS", "").split(",") if os.environ.get("ALLOWED_DAG_IDS") else None


@app.post("/pubsub-push")
async def receive(request: Request):
    envelope = await request.json()
    pubsub_message = envelope["message"]

    raw_data = base64.b64decode(pubsub_message["data"]).decode("utf-8")
    payload = json.loads(raw_data)

    publish_time = datetime.fromisoformat(
        pubsub_message["publishTime"].replace("Z", "+00:00")
    )
    age_minutes = (datetime.now(timezone.utc) - publish_time).total_seconds() / 60

    if age_minutes > 10:
        print(f"Skipping stale message. Age={age_minutes:.1f} minutes")
        return {"status": "stale"}

    dag_id = payload.get("dag_id")
    if ALLOWED_DAG_IDS and dag_id not in ALLOWED_DAG_IDS:
        print(f"Skipping dag_id={dag_id}, not in ALLOWED_DAG_IDS")
        return {"status": "ignored_dag"}

    future = publisher.publish(
        processing_topic_path, json.dumps(payload).encode("utf-8")
    )
    print(f"Relayed to processing topic: {future.result()}")

    return {"status": "relayed"}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}