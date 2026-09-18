import argparse
import json
from google.cloud import pubsub_v1

PROJECT_ID = "dag-failure-agent-505623"
TOPIC_ID = "dagfailures-processing"

def publish_message(args):
    log_content = None
    if args.log_file:
        with open(args.log_file, "r", encoding="utf-8") as f:
            log_content = f.read()

    payload = {
        "dag_id": args.dag_id,
        "task_id": args.task_id,
        "run_id": args.run_id,
        "target_file": args.target_file,
        "synthetic_task_logs": log_content or "sample log for verification",
        "github_repo": args.github_repo,
    }

    payload = {k: v for k, v in payload.items() if v is not None}

    publisher_client = pubsub_v1.PublisherClient()
    topic_path = publisher_client.topic_path(PROJECT_ID, TOPIC_ID)
    data = json.dumps(payload).encode("utf-8")
    future = publisher_client.publish(topic_path, data)

    print(f"Published message ID: {future.result(timeout=30)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish dynamic test message to Pub/Sub.")
    parser.add_argument("dag_id", help="DAG identifier")
    parser.add_argument("task_id", help="Task identifier")
    parser.add_argument("target_file", help="Path to target python DAG file")
    parser.add_argument("log_file", nargs="?", default=None, help="Path to log file (optional)")
    parser.add_argument("--run-id", default="manual-cli-test", help="Airflow run ID")
    parser.add_argument("--github-repo", default="AdityaShett/dag-failure-agent", help="GitHub repo (owner/repo)")

    args = parser.parse_args()
    publish_message(args)