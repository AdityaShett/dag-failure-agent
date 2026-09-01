import os
import time
from datetime import datetime, timedelta, timezone
from google.cloud import logging as cloud_logging


def _parse_failure_time_from_run_id(run_id: str) -> datetime:
    """Airflow run_ids look like 'manual__2026-08-19T00:22:48.048167+00:00'
    or 'scheduled__2026-08-19T00:00:00+00:00'. Fall back to now() if the
    format doesn't match (e.g. a synthetic test run_id)."""
    _, _, ts = run_id.partition("__")
    if not ts:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return datetime.now(timezone.utc)


def fetch_task_logs(dag_id: str, task_id: str, run_id: str, window_minutes: int = 15) -> str:
    project = os.environ.get("GCP_PROJECT") or os.environ["PROJECT_ID"]
    client = cloud_logging.Client(project=project)

    failure_time = _parse_failure_time_from_run_id(run_id)
    start = (failure_time - timedelta(minutes=window_minutes)).isoformat()
    end = (failure_time + timedelta(minutes=1)).isoformat()

    filter_str = (
        'resource.type="cloud_composer_environment" '
        f'AND labels."workflow"="{dag_id}" '
        f'AND labels."task-id"="{task_id}" '
        f'AND timestamp>="{start}" AND timestamp<="{end}"'
    )

    for attempt in range(3):
        entries = list(client.list_entries(
            filter_=filter_str, order_by=cloud_logging.DESCENDING, max_results=200
        ))
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