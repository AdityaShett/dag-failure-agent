import os
import time
from datetime import datetime, timedelta, timezone
from google.cloud import logging as cloud_logging
from google.api_core.exceptions import ResourceExhausted


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


def fetch_task_logs(filter_str: str) -> str:
    """Fetches task logs from Google Cloud Logging with exponential backoff for rate limits."""
    client = cloud_logging.Client()
    max_retries = 5
    base_delay = 2

    for attempt in range(max_retries):
        try:
            entries = list(
                client.list_entries(
                    filter_=filter_str,
                    order_by=cloud_logging.DESCENDING,
                    max_results=200,
                )
            )
            if entries:
                lines = [str(e.payload) for e in entries]
                return "\n".join(reversed(lines))
            return ""
        except ResourceExhausted as e:
            if attempt == max_retries - 1:
                raise e
            sleep_time = base_delay * (2 ** attempt)
            time.sleep(sleep_time)
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(base_delay * (2 ** attempt))

    return ""


def fetch_dag_source(dag_id: str, github_repo: str, target_file: str) -> str:
    from github import Auth, Github
    from agent.repo_config import get_github_token

    auth = Auth.Token(get_github_token(github_repo))
    gh = Github(auth=auth)
    repo = gh.get_repo(github_repo)
    contents = repo.get_contents(target_file)
    return contents.decoded_content.decode("utf-8")