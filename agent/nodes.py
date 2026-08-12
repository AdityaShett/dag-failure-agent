import os
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

    source = fetch_dag_source(state["dag_id"])

    return {
        "task_logs": logs,
        "dag_source": source,
    }


def retrieve_knowledge(state: dict) -> dict:
    try:
        if not os.path.isdir("chroma_kb"):
            return {"retrieved_knowledge": []}

        from langchain_community.vectorstores import Chroma
        from langchain_google_vertexai import VertexAIEmbeddings

        embeddings = VertexAIEmbeddings(
            model_name="text-embedding-005"
        )

        store = Chroma(
            persist_directory="chroma_kb",
            embedding_function=embeddings
        )

        retriever = store.as_retriever(
            search_kwargs={"k": 5}
        )

        query = (
            f"Airflow failure: {state['task_id']} "
            f"in {state['dag_id']}. "
            f"Logs: {state.get('task_logs', '')[-1500:]}"
        )

        hits = retriever.invoke(query)

        return {
            "retrieved_knowledge": [
                h.page_content for h in hits
            ]
        }

    except Exception:
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
            "confidence": "low",
        }

    try:
        prompt = [
            SystemMessage(
                content=(
                    "Propose the smallest possible safe fix, "
                    "as a git diff only. "
                    "If not confident, output "
                    "NO_CONFIDENT_FIX instead of guessing."
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

        fix = str(response.content)

        return {
            "proposed_fix": fix,
            "confidence": (
                "low"
                if "NO_CONFIDENT_FIX" in fix
                else "high"
            ),
        }

    except Exception as e:
        return {
            "proposed_fix": f"NO_CONFIDENT_FIX\n\nError: {str(e)}",
            "confidence": "low",
        }