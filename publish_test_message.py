import json, sys, uuid
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("dag-failure-agent-505623", "dagfailures-processing")

def publish(dag_id, task_id, target_file, logs, github_repo="AdityaShett/dag-failure-agent"):
    payload = {
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": f"e2e-test-{uuid.uuid4().hex[:8]}",
        "try_number": 1,
        "github_repo": github_repo,
        "target_file": target_file,
        "synthetic_task_logs": logs,
    }
    future = publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))
    print(f"[{dag_id}.{task_id}] run_id={payload['run_id']} msg_id={future.result()}")

if __name__ == "__main__":
    dag_id, task_id, target_file, log_file = sys.argv[1:5]
    with open(log_file) as f:
        logs = f.read()
    publish(dag_id, task_id, target_file, logs)