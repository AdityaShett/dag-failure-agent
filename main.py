import base64
import json

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

    dag_id = payload.get("dag_id")
    github_repo = payload.get("github_repo")

    # Automatically map DAG -> file
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