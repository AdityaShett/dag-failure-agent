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
        parsed = datetime.fromisoformat(ts)
    except ValueError:
        return datetime.now(timezone.utc)
    # An Airflow run_id can carry a naive timestamp; treat it as UTC rather
    # than letting astimezone() silently apply the container's local zone.
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def build_task_log_filter(dag_id: str, task_id: str, run_id: str = None,
                          lookback_minutes: int = 120) -> str:
    """Builds the Cloud Logging filter for one failed task attempt.

    This exists because callers kept trying to invoke fetch_task_logs with
    (dag_id, task_id, run_id) -- which is the natural thing to want -- while
    the function only ever accepted a single prebuilt filter string. Rather
    than change fetch_task_logs' contract (it is also called with
    hand-written filters), the three-argument form now has a home.

    The time window is anchored on the run_id when it carries an Airflow
    timestamp, falling back to a recent window otherwise, so this does not
    scan the whole retention period on every call.
    """
    failure_time = _parse_failure_time_from_run_id(run_id or "")
    start = failure_time - timedelta(minutes=lookback_minutes)
    end = failure_time + timedelta(minutes=lookback_minutes)

    parts = [
        f'timestamp >= "{start.astimezone(timezone.utc).isoformat()}"',
        f'timestamp <= "{end.astimezone(timezone.utc).isoformat()}"',
    ]
    if dag_id:
        parts.append(f'textPayload:"{dag_id}"')
    if task_id:
        parts.append(f'textPayload:"{task_id}"')
    return " AND ".join(parts)


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


def fetch_dag_source(dag_id: str, github_repo: str, target_file: str,
                     ref: str = None) -> str:
    """Reads a DAG file out of GitHub.

    `ref` is the important part: without it PyGithub reads the repo's
    DEFAULT branch. That is what produced the "fetch_dag_source is returning
    stale/wrong DAG code" symptom -- the benchmark rewrite lived on
    test/verify-secret-fix and was never merged, so the publisher sent logs
    for the new dag2.py while this read the old dag2.py off main, and the
    model was asked to explain a real traceback against a file it did not
    come from.

    Pass a ref explicitly, or set DAG_SOURCE_REF in the worker's env, to
    pin the worker to a branch until it merges.
    """
    from github import Auth, Github
    from agent.repo_config import get_github_token

    auth = Auth.Token(get_github_token(github_repo))
    gh = Github(auth=auth)
    repo = gh.get_repo(github_repo)

    resolved_ref = ref or repo.default_branch
    try:
        contents = repo.get_contents(target_file, ref=resolved_ref)
    except Exception as e:
        if ref and ref != repo.default_branch:
            print(f"WARNING: {target_file}@{ref} unavailable ({e!r}); "
                  f"falling back to {repo.default_branch}")
            contents = repo.get_contents(target_file, ref=repo.default_branch)
            resolved_ref = repo.default_branch
        else:
            raise

    print(f"Fetched {target_file} from {github_repo}@{resolved_ref}")
    return contents.decoded_content.decode("utf-8")
