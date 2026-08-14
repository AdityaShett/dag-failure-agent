from google.cloud import pubsub_v1
import json
import os
import logging

publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(
    os.environ.get("GCP_PROJECT"),
    "dag-failures-v2"
)

CONFIG_PATH = os.path.join(
    os.path.dirname(__file__),
    "config.json"
)

with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

def notify_on_failure(context):
    ti = context["task_instance"]

    message = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context["run_id"],
        "try_number": ti.try_number,

        "github_repo": config["github_repo"]

    }

    logging.info(f"Failure payload: {message}")

    try:
        future = publisher.publish(
            topic_path,
            json.dumps(message).encode("utf-8")
        )

        logging.info(
            f"Published failure event: {future.result()}"
        )

    except Exception as e:
        logging.exception(
            f"Failed to publish Pub/Sub message: {e}"
        )