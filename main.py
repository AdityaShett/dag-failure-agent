import os
import base64
import json
from datetime import datetime, timezone
from fastapi import FastAPI, Request

from agent.graph import app as agent_graph

api = FastAPI()


@api.post("/pubsub-push")
async def handle_pubsub_push(request: Request):
    envelope = await request.json()
    print(f"Received envelope: {envelope}")

    pubsub_message = envelope["message"]

    raw_data = base64.b64decode(
        pubsub_message["data"]
    ).decode("utf-8")

    payload = json.loads(raw_data)

    publish_time = datetime.fromisoformat(
        pubsub_message["publishTime"].replace("Z", "+00:00")
    )

    age_minutes = (
        datetime.now(timezone.utc) - publish_time
    ).total_seconds() / 60

    # Ignore stale messages older than 10 minutes
    if age_minutes > 10:
        print(
            f"Skipping stale message. "
            f"Age={age_minutes:.1f} minutes"
        )
        return {"status": "stale"}

    dag_id = payload.get("dag_id")
    github_repo = payload.get("github_repo")

    ALLOWED_DAG_IDS = os.environ.get("ALLOWED_DAG_IDS", "dag1").split(",")
    if dag_id not in ALLOWED_DAG_IDS:
        print(f"Skipping dag_id={dag_id}, not in ALLOWED_DAG_IDS")
        return {"status": "ignored_dag"}

    # Automatic DAG -> file mapping
    target_file = f"tests/{dag_id}.py"

    print(f"Decoded payload: {payload}")
    print(f"DAG={dag_id}")
    print(f"REPO={github_repo}")
    print(f"FILE={target_file}")

    result = agent_graph.invoke(
        {
            "dag_id": dag_id,
            "task_id": payload.get("task_id"),
            "run_id": payload.get("run_id"),
            "try_number": payload.get("try_number", 1),
            "github_repo": github_repo,
            "target_file": target_file,
        }
    )

    print(f"Graph result: {result}")

    return {
        "status": "processed",
        "pr_url": result.get("pr_url"),
    }
