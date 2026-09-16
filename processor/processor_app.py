import base64
import json
import logging
import os

from fastapi import FastAPI, Request

from agent.graph import app as agent_graph
from agent import results_log
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_storage_client = storage.Client()
_DEDUP_BUCKET = os.environ.get("DEDUP_BUCKET")

def _already_processed(run_id: str, task_id: str) -> bool:
    if not _DEDUP_BUCKET or not run_id or not task_id:
        return False
    bucket = _storage_client.bucket(_DEDUP_BUCKET)
    blob = bucket.blob(f"processed/{run_id}-{task_id}")
    if blob.exists():
        return True
    blob.upload_from_string("1")
    return False

_REPOS_CONFIG_PATH = os.environ.get("REPOS_CONFIG_PATH", "config/repos.json")
_repos_config_cache = None

def _load_repos_config() -> dict:
    global _repos_config_cache
    if _repos_config_cache is not None:
        return _repos_config_cache
    try:
        with open(_REPOS_CONFIG_PATH, "r") as f:
            _repos_config_cache = json.load(f)
    except Exception as e:
        logger.warning(f"Could not load {_REPOS_CONFIG_PATH} ({e!r}); no per-repo dag path templates available")
        _repos_config_cache = {}
    return _repos_config_cache


def _resolve_target_file(github_repo: str, dag_id: str) -> str:
    repos_config = _load_repos_config()
    repo_entry = repos_config.get(github_repo) or repos_config.get("default") or {}
    template = repo_entry.get("dag_path_template", "tests/{dag_id}.py")
    return template.format(dag_id=dag_id)

app = FastAPI()


@app.post("/process")
async def process(request: Request):
    try:
        raw_data = await request.body()
        envelope = json.loads(raw_data)
        pubsub_message = envelope.get("message", {})

        # Decode the base64 Pub/Sub payload data
        message_bytes = base64.b64decode(pubsub_message.get("data", ""))
        message_data = message_bytes.decode("utf-8")

        payload = json.loads(message_data)
    except Exception as e:
        logger.error(f"Could not decode/parse Pub/Sub message data: {e}")
        # Return 200 so Pub/Sub drops malformed messages and stops infinite retry loops
        return {"status": "error", "message": f"Invalid payload: {e}"}

    run_id = payload.get("run_id")
    task_id = payload.get("task_id")

    if _already_processed(run_id, task_id):
        logger.info(f"Skipping duplicate: run_id={run_id} task_id={task_id}")
        return {"status": "duplicate_skipped"}

    dag_id = payload.get("dag_id")
    github_repo = payload.get("github_repo")

    target_file = payload.get("target_file") or _resolve_target_file(github_repo, dag_id)

    logger.info(f"Processing: DAG={dag_id} REPO={github_repo} FILE={target_file}")

    # The graph call is wrapped so a crash inside it cannot become a poison
    # pill. Previously only envelope parsing was guarded: any exception in
    # the graph (the fetch_task_logs TypeError, a GitHub 404, a Vertex
    # quota error) escaped as an unhandled 500, Pub/Sub nacked the message
    # and redelivered it -- for up to the 7-day retention window. That is
    # what the "dozens of PRs referencing old task names over 1.5 hours"
    # backlog actually was.
    try:
        result = agent_graph.invoke(
            {
                "dag_id": dag_id,
                "task_id": payload.get("task_id"),
                "run_id": run_id,
                "try_number": payload.get("try_number", 1),
                "github_repo": github_repo,
                "target_file": target_file,
                "synthetic_task_logs": payload.get("synthetic_task_logs"),
                "source_ref": payload.get("source_ref") or os.environ.get("DAG_SOURCE_REF") or None,
                # Benchmark metadata, carried through so a result row can be
                # scored against what the scenario expected.
                "scenario_id": payload.get("scenario_id"),
                "difficulty": payload.get("difficulty"),
                "failure_type": payload.get("failure_type"),
                "expected_outcome": payload.get("expected_outcome"),
            }
        )
    except Exception as e:
        logger.exception(
            f"Graph failed for run_id={run_id} dag_id={dag_id}: {e}"
        )
        # 200 = ack. The message is bad or the run is unrecoverable; retrying
        # it identically will not help, and the traceback is in the logs.
        return {"status": "error", "stage": "graph", "message": str(e)}

    logger.info(f"Graph result: {result}")

    actual_outcome = "PR_CREATED" if result.get("pr_url") else "NO_CONFIDENT_FIX"
    results_log.record_run({**payload, **result}, actual_outcome)

    expected = payload.get("expected_outcome")
    if expected and expected != actual_outcome:
        logger.warning(
            f"UNEXPECTED OUTCOME scenario={payload.get('scenario_id')} "
            f"expected={expected} actual={actual_outcome} "
            f"confidence={result.get('confidence_score')} "
            f"reason={result.get('gate_reason')}"
        )

    return {
        "status": "processed",
        "outcome": actual_outcome,
        "pr_url": result.get("pr_url"),
        "confidence_score": result.get("confidence_score"),
        "confidence_tier": result.get("confidence_tier"),
        "gate_reason": result.get("gate_reason"),
    }


@app.get("/weights")
async def weights():
    """What the running container actually loaded from config/weights.json.

    Worth having: 'is the deployed worker using the weights I just
    committed?' was previously only answerable by reading cold-start logs.
    """
    from agent import nodes
    return {
        "weights": nodes.CONFIDENCE_WEIGHTS,
        "threshold": nodes.CONFIDENCE_THRESHOLD,
        "source_ref": os.environ.get("DAG_SOURCE_REF") or "(repo default branch)",
    }


@app.post("/weights/reload")
async def weights_reload():
    from agent import nodes
    return {"weights": nodes.reload_confidence_weights()}


@app.get("/status-check")
async def status_check():
    return {"status": "ok"}

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
