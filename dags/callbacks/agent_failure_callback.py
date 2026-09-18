# dags/callbacks/agent_failure_callback.py
"""
Publishes a real task failure to the dag-failure-agent pipeline.

Wire this in via Step 3. Do NOT set "synthetic_task_logs" in the payload --
its absence is what makes processor/agent/nodes.py's collect_context call the
real fetch_task_logs() (Cloud Logging) instead of the synthetic-test bypass.
"""
import json
import os

from google.cloud import pubsub_v1

_PROJECT = os.environ.get("GCP_PROJECT", "dag-failure-agent-cap")
_TOPIC_NAME = os.environ.get("AGENT_RECEIVER_TOPIC", "dagfailures-receiver")
_GITHUB_REPO = os.environ.get("AGENT_GITHUB_REPO", "AdityaShett/dag-failure-agent")
_DAG_PATH_TEMPLATE = os.environ.get("AGENT_DAG_PATH_TEMPLATE", "dags/{dag_id}.py")

_publisher = pubsub_v1.PublisherClient()
_topic_path = _publisher.topic_path(_PROJECT, _TOPIC_NAME)


def notify_dag_failure_agent(context):
    """Airflow on_failure_callback signature: takes one dict, `context`."""
    ti = context["task_instance"]
    dag_run = context["dag_run"]

    payload = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": dag_run.run_id,
        "try_number": ti.try_number,
        "github_repo": _GITHUB_REPO,
        "target_file": _DAG_PATH_TEMPLATE.format(dag_id=ti.dag_id),
    }

    try:
        future = _publisher.publish(_topic_path, json.dumps(payload).encode("utf-8"))
        future.result(timeout=10)
        print(f"Published failure event for {ti.dag_id}.{ti.task_id} "
              f"(run_id={dag_run.run_id})")
    except Exception as e:
        # Never let a notification failure break the DAG's own failure handling.
        print(f"WARNING: could not publish failure event: {e!r}")