import os
from google.cloud import logging as cloud_logging

import time

def fetch_task_logs(dag_id: str, task_id: str, run_id: str) -> str:
    project_id = os.environ.get("GCP_PROJECT") or os.environ.get("PROJECT_ID")
    if not project_id:
        raise RuntimeError("GCP_PROJECT or PROJECT_ID must be set")

    client = cloud_logging.Client(project=project_id)
    filter_str = (
        'resource.type="cloud_composer_environment" '
        f'AND labels."workflow" = "{dag_id}" '
        f'AND labels."task-id" = "{task_id}" '
        f'AND labels."run-id" = "{run_id}"'
    )

    for attempt in range(3):
        entries = list(client.list_entries(filter_=filter_str, order_by=cloud_logging.DESCENDING, max_results=200))
        if entries:
            lines = [str(e.payload) for e in entries]
            return "\n".join(reversed(lines))
        if attempt < 2:
            time.sleep(10)

    return ""

def fetch_dag_source(dag_id: str, github_repo: str, target_file: str) -> str:
    from github import Auth, Github

    auth = Auth.Token(os.environ["GITHUB_TOKEN"])
    gh = Github(auth=auth)
    repo = gh.get_repo(github_repo)
    contents = repo.get_contents(target_file)
    return contents.decoded_content.decode("utf-8")
