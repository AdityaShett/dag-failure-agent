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

    print(f"Decoded payload: {payload}")

    result = agent_graph.invoke(
        {
            "dag_id": payload.get("dag_id"),
            "task_id": payload.get("task_id"),
            "run_id": payload.get("run_id"),
            "try_number": payload.get("try_number", 1),
        }
    )

    print(f"Graph result: {result}")

    return {
        "status": "processed",
        "pr_url": result.get("pr_url"),
    }
