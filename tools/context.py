import os
from google.cloud import logging as cloud_logging
from google.cloud import storage

def fetch_task_logs(dag_id: str, task_id: str, run_id: str) -> str:
    project_id = os.environ.get("GCP_PROJECT") or os.environ.get("PROJECT_ID")
    if not project_id:
        raise RuntimeError("GCP_PROJECT or PROJECT_ID must be set")
    
    client = cloud_logging.Client(project=project_id)
    filter_str = (
        'resource.type="cloud_composer_environment" '
        f'AND labels."workflow" = "{dag_id}" '
        f'AND labels."task-id" = "{task_id}" '
        f'AND labels."execution-date" = "{run_id}"'
    )
    entries = client.list_entries(filter_=filter_str, order_by=cloud_logging.DESCENDING, max_results=200)
    lines = [str(e.payload) for e in entries]
    return "\n".join(reversed(lines))

def fetch_dag_source(dag_id: str) -> str:
    bucket_name = os.environ["COMPOSER_BUCKET"]
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"dags/{dag_id}.py")
    return blob.download_as_text()
