import os
import re
import json
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery
from google.cloud import storage as gcs_storage
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import SystemMessage, HumanMessage
from tools.context import build_task_log_filter, fetch_task_logs, fetch_dag_source

from agent import confidence
from agent.diff_utils import apply_unified_diff
from unidiff.errors import UnidiffParseError


PROJECT_ID = os.environ.get("GCP_PROJECT")
LOCATION = os.environ.get("GCP_LOCATION", "global")
BQ_DATASET = os.environ.get("BQ_DATASET", "dag_failure_agent")
BQ_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.fix_history"

# Which git ref the worker reads DAG source from. Leave unset to use the
# repo's default branch. Set it (env: DAG_SOURCE_REF) to point the worker at
# a feature branch while that branch is still unmerged -- otherwise the
# publisher sends logs for the new DAGs while this reads the old ones off
# main, and every run is analysed against the wrong file.
DAG_SOURCE_REF = os.environ.get("DAG_SOURCE_REF", "").strip()
CONFIDENCE_SIGNALS_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.confidence_signals"

_bq_client = None
_outcomes_client_for_history = None


def _get_bq_client():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


llm = ChatGoogleGenerativeAI(
    model=os.environ.get("GEMINI_MODEL_NAME", "gemini-2.5-flash"),
    vertexai=True,
    temperature=0.7,
    project=PROJECT_ID,
    location=LOCATION,
    max_output_tokens=8192,
)


def collect_context(state: dict) -> dict:
    if state.get("synthetic_task_logs") is not None:
        logs = state["synthetic_task_logs"]
    else:
        # fetch_task_logs takes ONE Cloud Logging filter string. It used to be
        # called here with three positional args (dag_id, task_id, run_id),
        # which raised TypeError on every non-synthetic run before scoring
        # could happen -- and because processor_app.py didn't catch it, Pub/Sub
        # redelivered the same message for days.
        logs = fetch_task_logs(
            build_task_log_filter(
                dag_id=state["dag_id"],
                task_id=state["task_id"],
                run_id=state.get("run_id"),
            )
        )

    source_ref = state.get("source_ref") or DAG_SOURCE_REF or None
    source = fetch_dag_source(
        state["dag_id"],
        state["github_repo"],
        state["target_file"],
        ref=source_ref,
    )

    if not (source or "").strip():
        print(f"WARNING: empty DAG source for {state['target_file']} "
              f"(repo={state['github_repo']} ref={source_ref or 'default branch'})")

    return {
        "task_logs": logs,
        "dag_source": source,
        "source_ref": source_ref or "",
    }


def retrieve_knowledge(state: dict) -> dict:
    try:
        from langchain_google_vertexai import VertexAIEmbeddings

        embeddings = VertexAIEmbeddings(model_name="text-embedding-005")

        query_text = (
            f"Airflow failure: {state['task_id']} "
            f"in {state['dag_id']}. "
            f"Logs: {state.get('task_logs', '')[-1500:]}"
        )

        query_embedding = embeddings.embed_query(query_text)

        client = _get_bq_client()

        sql = f"""
            SELECT
                root_cause,
                proposed_fix,
                outcome,
                ML.DISTANCE(embedding, @query_embedding, 'COSINE') AS distance
            FROM `{BQ_TABLE}`
            WHERE outcome = 'merged'
            ORDER BY distance ASC
            LIMIT 5
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "query_embedding", "FLOAT64", query_embedding
                )
            ]
        )

        results = client.query(sql, job_config=job_config).result()

        hits = [
            f"Root cause: {row.root_cause}\nFix: {row.proposed_fix}"
            for row in results
        ]

        return {"retrieved_knowledge": hits}

    except Exception as e:
        print(f"BigQuery retrieval failed: {e}")
        return {"retrieved_knowledge": []}


def analyze_root_cause(state: dict) -> dict:
    try:
        knowledge = "\n---\n".join(
            state.get("retrieved_knowledge", [])
        )

        prompt = [
            SystemMessage(
                content=(
                    "You are an Airflow reliability engineer. "
                    "Find the exact root cause. "
                    "Point to the specific log lines and code lines "
                    "that prove it. "
                    "If unclear, say so instead of guessing."
                )
            ),
            HumanMessage(
                content=(
                    f"DAG: {state['dag_id']} | "
                    f"Task: {state['task_id']}\n\n"
                    f"--- LOGS ---\n"
                    f"{state.get('task_logs', '')[-4000:]}\n\n"
                    f"--- DAG CODE ---\n"
                    f"{state.get('dag_source', '')}\n\n"
                    f"--- SIMILAR PAST ISSUES ---\n"
                    f"{knowledge}\n\n"
                    "Give: "
                    "1) root cause, "
                    "2) evidence, "
                    "3) confidence (high/medium/low)."
                )
            ),
        ]

        response = llm.invoke(prompt)

        return {
            "root_cause": str(response.content)
        }

    except Exception as e:
        return {
            "root_cause": f"Analysis failed: {str(e)}"
        }


def _diff_context_exists(diff_text: str, dag_source: str) -> bool:
    """Trial-applies the diff against the real source using the exact same
    logic pr.py uses at apply-time. If it would fail there, it's not a
    usable fix -- catch it here instead of burning a GitHub branch/PR on
    something guaranteed to fall back."""
    try:
        apply_unified_diff(dag_source, diff_text)
        return True
    except (UnidiffParseError, ValueError, Exception):
        return False


def generate_fix(state: dict) -> dict:
    root_cause = state.get("root_cause")
    dag_source = state.get("dag_source", "")

    if not root_cause:
        return {
            "proposed_fix": "NO_CONFIDENT_FIX",
        }

    try:
        prompt = [
            SystemMessage(
                content=(
                    "Propose the smallest possible safe fix, as a git diff only.\n\n"
                    "Before proposing anything, check whether the root cause actually "
                    "matches the code shown below. If the root cause says the DAG ID "
                    "or task ID doesn't match the code, if the described bug is not "
                    "present in the current source, or if you're not certain the exact "
                    "lines you'd change still look the way the root cause assumes, do "
                    "NOT guess a diff — respond with NO_CONFIDENT_FIX instead.\n\n"
                    "Respond in EXACTLY this format, nothing else:\n"
                    "DIFF:\n"
                    "<the git diff, or the literal text NO_CONFIDENT_FIX "
                    "if you are not confident or the described bug isn't actually there>"
                )
            ),
            HumanMessage(
                content=(
                    f"Root cause:\n{root_cause}\n\n"
                    f"Current code:\n{dag_source}"
                )
            ),
        ]

        response = llm.invoke(prompt)
        text = str(response.content)

        fix = "NO_CONFIDENT_FIX"
        diff_match = re.search(r"DIFF:\s*(.*)", text, re.DOTALL)
        if diff_match:
            fix = diff_match.group(1).strip()

        if fix != "NO_CONFIDENT_FIX" and not _diff_context_exists(fix, dag_source):
            fix = "NO_CONFIDENT_FIX"

        return {
            "proposed_fix": fix,
        }

    except Exception as e:
        return {
            "proposed_fix": f"NO_CONFIDENT_FIX\n\nError: {str(e)}"
        }


# ---------------------------------------------------------------------------
# Confidence scoring
#
# All of the arithmetic now lives in agent/confidence.py so that the worker,
# the offline benchmark (run_benchmark_local.py) and the tuner
# (tune_weights.py) cannot drift apart. What used to be here computed
# s_logs = 1.0 if len(logs) > 50 and s_source = 1.0 if len(source) > 50,
# which made every real run score exactly 1.0 and made config/weights.json
# a no-op -- changing a weight could not change a decision.
# ---------------------------------------------------------------------------

CONFIDENCE_WEIGHTS = confidence.load_weights()
CONFIDENCE_THRESHOLD = CONFIDENCE_WEIGHTS["confidence_threshold"]


def reload_confidence_weights() -> dict:
    """Re-reads config/weights.json. The module-level load happens once at
    cold start; call this from a debug endpoint if you want to pick up a new
    config without a redeploy."""
    global CONFIDENCE_WEIGHTS, CONFIDENCE_THRESHOLD
    CONFIDENCE_WEIGHTS = confidence.load_weights()
    CONFIDENCE_THRESHOLD = CONFIDENCE_WEIGHTS["confidence_threshold"]
    return CONFIDENCE_WEIGHTS


def _weights_version() -> str:
    meta = CONFIDENCE_WEIGHTS.get("metadata") or {}
    return str(meta.get("fitted_at") or meta.get("source") or "unversioned")[:120]


def _history_stats(dag_id: str, task_id: str):
    """Returns (merge_rate, n_records) for this dag+task, or (None, 0) when
    there is no history. None -- not 0.5 -- so the scorer drops the signal
    and renormalises instead of injecting a made-up number."""
    global _outcomes_client_for_history
    bucket_name = os.environ.get("OUTCOMES_BUCKET")
    if not bucket_name or not dag_id or not task_id:
        return None, 0

    try:
        if _outcomes_client_for_history is None:
            _outcomes_client_for_history = gcs_storage.Client()
        bucket = _outcomes_client_for_history.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=f"history/{dag_id}-{task_id}/"))
    except Exception as e:
        print(f"WARNING: could not list history for {dag_id}-{task_id}: {e!r}")
        return None, 0

    if not blobs:
        return None, 0

    merged = 0
    counted = 0
    for blob in blobs:
        try:
            record = json.loads(blob.download_as_text())
        except Exception as e:
            print(f"WARNING: failed to read history blob {blob.name}: {e!r}")
            continue
        counted += 1
        if record.get("merged"):
            merged += 1

    if not counted:
        return None, 0
    return merged / counted, counted


def _log_confidence_signals(state: dict, signals: dict, score: float, tier: str) -> str:
    """Logs every scored run (not just ones that open a PR) to BigQuery so
    weights can later be fitted against real outcomes. Returns record_id so
    it can be carried into the PR record for outcome linking.

    Writes one column per signal (see migrations/001_*.sql). The old
    s_logs/s_source columns are left NULL: they held the two degenerate
    length checks and are not worth tuning against."""
    record_id = str(uuid.uuid4())

    row = {
        "record_id": record_id,
        "dag_id": state.get("dag_id", ""),
        "task_id": state.get("task_id", ""),
        "run_id": state.get("run_id", ""),
        "scenario_id": state.get("scenario_id"),
        "confidence_score": score,
        "confidence_tier": tier,
        "weights_version": _weights_version(),
        "pr_number": None,   # backfilled on the confidence_outcomes row pr.py writes
        "outcome": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": None,
    }
    for name in confidence.ALL_SIGNALS:
        row[f"s_{name}"] = signals.get(name)

    try:
        errors = _get_bq_client().insert_rows_json(CONFIDENCE_SIGNALS_TABLE, [row])
        if errors:
            print(f"ERROR: confidence_signals insert failed for {record_id}: {errors}")
            return None
    except Exception as e:
        print(f"ERROR: failed to log confidence signals for {record_id}: {e!r}")
        return None

    return record_id


def _mark_confidence_no_pr(record_id: str, reason: str = None):
    if not record_id:
        return
    row = {
        "record_id": record_id,
        "outcome": "no_pr",
        "pr_number": None,
        "fallback_reason": reason,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    table = f"{PROJECT_ID}.{BQ_DATASET}.confidence_outcomes"
    try:
        errors = _get_bq_client().insert_rows_json(table, [row])
        if errors:
            print(f"WARNING: confidence_outcomes insert failed: {errors}")
    except Exception as e:
        print(f"WARNING: failed to record no_pr outcome: {e!r}")


def compute_confidence(state: dict) -> dict:
    history_rate, history_n = _history_stats(
        state.get("dag_id"), state.get("task_id"))

    retrieved = state.get("retrieved_knowledge")
    retrieval_hits = len(retrieved) if retrieved is not None else None

    result = confidence.score_run(
        task_logs=state.get("task_logs", "") or "",
        dag_source=state.get("dag_source", "") or "",
        weights_cfg=CONFIDENCE_WEIGHTS,
        dag_id=state.get("dag_id", "") or "",
        task_id=state.get("task_id", "") or "",
        history_merge_rate=history_rate,
        retrieval_hits=retrieval_hits,
    )

    signals = result["signals"]
    print(
        f"CONFIDENCE dag={state.get('dag_id')} task={state.get('task_id')} "
        f"score={result['score']} tier={result['tier']} "
        f"threshold={CONFIDENCE_THRESHOLD} "
        f"meets_threshold={result['meets_threshold']} "
        f"history_n={history_n} signals={{"
        + ", ".join(f"{k}={'n/a' if v is None else round(v, 3)}"
                    for k, v in signals.items())
        + "}"
    )

    record_id = _log_confidence_signals(
        state, signals, result["score"], result["tier"])

    return {
        "confidence_score": result["score"],
        "confidence_tier": result["tier"],
        "confidence_signals": signals,
        "confidence_contributions": result["contributions"],
        "confidence_threshold": CONFIDENCE_THRESHOLD,
        "meets_confidence_threshold": result["meets_threshold"],
        "confidence_record_id": record_id,
    }
