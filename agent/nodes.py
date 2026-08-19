import os
import re
import json
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import SystemMessage, HumanMessage
from tools.context import fetch_task_logs, fetch_dag_source

PROJECT_ID = os.environ.get("GCP_PROJECT")
LOCATION = os.environ.get("GCP_LOCATION", "global")

llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-pro",
    vertexai=True,
    temperature=0.7,
    project=PROJECT_ID,
    location=LOCATION,
    max_output_tokens=2048,
)


def collect_context(state: dict) -> dict:
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


import time

_kb_store = None
_kb_loaded_at = 0.0
_KB_TTL_SECONDS = 900  

def _load_knowledge_base():
    global _kb_store, _kb_loaded_at

    now = time.time()
    if _kb_store is not None and (now - _kb_loaded_at) < _KB_TTL_SECONDS:
        return _kb_store

    bucket_name = os.environ.get("OUTCOMES_BUCKET")
    if not bucket_name:
        return None

    try:
        from google.cloud import storage as gcs_storage
        from langchain_community.vectorstores import Chroma
        from langchain_google_vertexai import VertexAIEmbeddings
        from langchain_core.documents import Document

        client = gcs_storage.Client()
        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix="history/"))

        documents = []
        for blob in blobs:
            if not blob.name.endswith(".json"):
                continue
            try:
                record = json.loads(blob.download_as_text())
            except Exception:
                continue

            if not record.get("merged"):
                continue

            root_cause = record.get("root_cause", "")
            proposed_fix = record.get("proposed_fix", "")
            if not root_cause and not proposed_fix:
                continue

            content = f"Root cause:\n{root_cause}\n\nFix:\n{proposed_fix}"
            documents.append(Document(
                page_content=content,
                metadata={
                    "dag_id": record.get("dag_id", ""),
                    "task_id": record.get("task_id", ""),
                    "pr_number": record.get("pr_number", 0),
                },
            ))

        if not documents:
            _kb_store = None
            _kb_loaded_at = now
            return None

        embeddings = VertexAIEmbeddings(
            model_name="text-embedding-005",
            project=PROJECT_ID,
            location=LOCATION,
        )

        store = Chroma.from_documents(documents, embedding=embeddings)
        _kb_store = store
        _kb_loaded_at = now
        print(f"Knowledge base rebuilt: {len(documents)} merged-fix document(s)")
        return store

    except Exception as e:
        print(f"WARNING: failed to build knowledge base: {e!r}")
        return None


def retrieve_knowledge(state: dict) -> dict:
    try:
        store = _load_knowledge_base()
        if store is None:
            return {"retrieved_knowledge": []}

        query = (
            f"Airflow failure: {state['task_id']} "
            f"in {state['dag_id']}. "
            f"Logs: {state.get('task_logs', '')[-1500:]}"
        )

        hits = store.similarity_search(query, k=5)

        return {
            "retrieved_knowledge": [h.page_content for h in hits]
        }

    except Exception as e:
        print(f"WARNING: retrieve_knowledge failed: {e!r}")
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

from google.cloud import storage as gcs_storage

_outcomes_client_for_history = None

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

    # No PR-outcome tracking exists yet (would need a webhook on PR
    # merge/close to know if past fixes were actually accepted), so
    # this stays a neutral placeholder until that's built.
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

    return {
        "confidence_score": round(score, 3),
        "confidence_tier": tier,
    }