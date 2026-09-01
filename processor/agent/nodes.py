import os
import re
import json
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery
from google.cloud import storage as gcs_storage
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import SystemMessage, HumanMessage
from tools.context import fetch_task_logs, fetch_dag_source


PROJECT_ID = os.environ.get("GCP_PROJECT")
LOCATION = os.environ.get("GCP_LOCATION", "global")
BQ_DATASET = os.environ.get("BQ_DATASET", "dag_failure_agent")
BQ_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.fix_history"
CONFIDENCE_SIGNALS_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.confidence_signals"

_bq_client = None
_outcomes_client_for_history = None


def _get_bq_client():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-pro",
    vertexai=True,
    temperature=0.7,
    project=PROJECT_ID,
    location=LOCATION,
    max_output_tokens=2048,
)


def collect_context(state: dict) -> dict:
    if state.get("synthetic_task_logs") is not None:
        logs = state["synthetic_task_logs"]
    else:
        logs = fetch_task_logs(
            state["dag_id"],
            state["task_id"],
            state["run_id"]
        )

    source = fetch_dag_source(
        state["dag_id"],
        state["github_repo"],
        state["target_file"],
    )

    return {
        "task_logs": logs,
        "dag_source": source,
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


def generate_fix(state: dict) -> dict:
    root_cause = state.get("root_cause")
    dag_source = state.get("dag_source", "")

    if not root_cause:
        return {
            "proposed_fix": "NO_CONFIDENT_FIX",
            "llm_confidence": 0.0,
        }

    try:
        prompt = [
            SystemMessage(
                content=(
                    "Propose the smallest possible safe fix, as a git diff only. "
                    "Respond in EXACTLY this format, nothing else:\n"
                    "CONFIDENCE: <a decimal between 0.0 and 1.0, how confident "
                    "you are this fix is correct>\n"
                    "DIFF:\n"
                    "<the git diff, or the literal text NO_CONFIDENT_FIX "
                    "if you are not confident>"
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

        llm_confidence = 0.0
        fix = "NO_CONFIDENT_FIX"

        conf_match = re.search(r"CONFIDENCE:\s*([0-9.]+)", text)
        if conf_match:
            try:
                llm_confidence = max(0.0, min(1.0, float(conf_match.group(1))))
            except ValueError:
                llm_confidence = 0.0

        diff_match = re.search(r"DIFF:\s*(.*)", text, re.DOTALL)
        if diff_match:
            fix = diff_match.group(1).strip()

        return {
            "proposed_fix": fix,
            "llm_confidence": llm_confidence,
        }

    except Exception as e:
        return {
            "proposed_fix": f"NO_CONFIDENT_FIX\n\nError: {str(e)}",
            "llm_confidence": 0.0,
        }


CONFIDENCE_WEIGHTS = {
    "llm": 0.35,
    "retrieval": 0.20,
    "history": 0.20,
    "logs": 0.15,
    "source": 0.10,
}


def _fetch_history_score(dag_id: str, task_id: str) -> float:
    global _outcomes_client_for_history
    bucket_name = os.environ.get("OUTCOMES_BUCKET")
    if not bucket_name or not dag_id or not task_id:
        return 0.5

    if _outcomes_client_for_history is None:
        _outcomes_client_for_history = gcs_storage.Client()

    bucket = _outcomes_client_for_history.bucket(bucket_name)
    prefix = f"history/{dag_id}-{task_id}/"
    blobs = list(bucket.list_blobs(prefix=prefix))

    if not blobs:
        return 0.5

    merged_count = 0
    for blob in blobs:
        try:
            record = json.loads(blob.download_as_text())
            if record.get("merged"):
                merged_count += 1
        except Exception as e:
            print(f"WARNING: failed to read history blob {blob.name}: {e!r}")
            continue

    return merged_count / len(blobs)


def _log_confidence_signals(state: dict, signals: dict, score: float, tier: str) -> str:
    """Logs every scored run (not just ones that open a PR) to BigQuery for
    later empirical weight derivation. Returns record_id so it can be
    carried forward into the pending PR record for outcome linking."""
    record_id = str(uuid.uuid4())

    row = {
        "record_id": record_id,
        "dag_id": state.get("dag_id", ""),
        "task_id": state.get("task_id", ""),
        "run_id": state.get("run_id", ""),
        "s_llm": signals.get("s_llm"),
        "s_retrieval": signals.get("s_retrieval"),
        "s_history": signals.get("s_history"),
        "s_logs": signals.get("s_logs"),
        "s_source": signals.get("s_source"),
        "confidence_score": score,
        "confidence_tier": tier,
        "pr_number": None,
        "outcome": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": None,
    }

    try:
        errors = _get_bq_client().insert_rows_json(CONFIDENCE_SIGNALS_TABLE, [row])
        if errors:
            print(f"WARNING: confidence_signals insert failed: {errors}")
    except Exception as e:
        print(f"WARNING: failed to log confidence signals: {e!r}")

    return record_id

def _mark_confidence_no_pr(record_id: str):
    if not record_id:
        return
    row = {
        "record_id": record_id,
        "outcome": "no_pr",
        "pr_number": None,
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
    llm_confidence = state.get("llm_confidence", 0.0)

    retrieved = state.get("retrieved_knowledge", [])
    s_retrieval = min(1.0, len(retrieved) / 5)

    task_logs = state.get("task_logs", "") or ""
    if len(task_logs) > 50:
        s_logs = 1.0
    elif task_logs:
        s_logs = 0.3
    else:
        s_logs = 0.0

    dag_source = state.get("dag_source", "") or ""
    s_source = 1.0 if len(dag_source) > 50 else 0.0

    s_history = _fetch_history_score(state.get("dag_id"), state.get("task_id"))

    score = (
        CONFIDENCE_WEIGHTS["llm"] * llm_confidence
        + CONFIDENCE_WEIGHTS["retrieval"] * s_retrieval
        + CONFIDENCE_WEIGHTS["history"] * s_history
        + CONFIDENCE_WEIGHTS["logs"] * s_logs
        + CONFIDENCE_WEIGHTS["source"] * s_source
    )

    if score >= 0.75:
        tier = "high"
    elif score >= 0.50:
        tier = "medium"
    else:
        tier = "low"

    signals = {
        "s_llm": llm_confidence,
        "s_retrieval": s_retrieval,
        "s_history": s_history,
        "s_logs": s_logs,
        "s_source": s_source,
    }

    record_id = _log_confidence_signals(state, signals, round(score, 3), tier)

    return {
        "confidence_score": round(score, 3),
        "confidence_tier": tier,
        "confidence_record_id": record_id,
    }