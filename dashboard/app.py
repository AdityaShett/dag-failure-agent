import os
from typing import Optional

import streamlit as st
from google.cloud import bigquery
import pandas as pd

PROJECT = os.environ["GCP_PROJECT"]
DATASET = os.environ.get("BQ_DATASET", "dag_failure_agent")

client = bigquery.Client(project=PROJECT)

st.set_page_config(page_title="DAG Failure Agent Dashboard", layout="wide")
st.title("DAG Failure Agent — Status Dashboard")


def safe_query(query: str, empty_msg: str) -> Optional[pd.DataFrame]:
    """Runs a query defensively. This project's tables have repeatedly been
    missing, empty, or mid-migration (see the diagnostic changelog) -- a bare
    query here would take down the whole dashboard instead of just one panel.
    """
    try:
        df = client.query(query).to_dataframe()
    except Exception as e:
        st.warning(
            f"Couldn't load this section ({type(e).__name__}). "
            f"The underlying table may not exist yet or the query needs a schema update."
        )
        return None
    if df.empty:
        st.info(empty_msg)
        return None
    return df


@st.cache_data(ttl=60)
def load_scored_runs() -> Optional[pd.DataFrame]:
    # confidence_outcomes only ever holds record_id/outcome/pr_number/
    # diff_applied/fallback_reason/updated_at -- dag_id, task_id, and the
    # score/tier live on confidence_signals instead. A PR's lifecycle can
    # produce more than one confidence_outcomes row over time (an "opened"
    # row, then later a "merged"/"rejected" row), so we take only the
    # latest row per record_id before joining.
    query = f"""
        WITH latest_outcome AS (
            SELECT
                record_id, outcome, pr_number, diff_applied, fallback_reason, updated_at,
                ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY updated_at DESC) AS rn
            FROM `{PROJECT}.{DATASET}.confidence_outcomes`
        )
        SELECT
            s.dag_id, s.task_id, s.confidence_score, s.confidence_tier, s.created_at,
            o.outcome, o.pr_number, o.diff_applied, o.fallback_reason, o.updated_at
        FROM `{PROJECT}.{DATASET}.confidence_signals` s
        JOIN latest_outcome o ON o.record_id = s.record_id AND o.rn = 1
        ORDER BY o.updated_at DESC
        LIMIT 200
    """
    return safe_query(query, empty_msg="No scored runs with a recorded outcome yet.")


@st.cache_data(ttl=60)
def load_fix_history() -> Optional[pd.DataFrame]:
    query = f"""
        SELECT dag_id, task_id, outcome, COUNT(*) AS count
        FROM `{PROJECT}.{DATASET}.fix_history`
        GROUP BY dag_id, task_id, outcome
        ORDER BY count DESC
    """
    return safe_query(query, empty_msg="No merged/rejected fix history recorded yet.")


outcomes_df = load_scored_runs()

st.subheader("Overview")
col1, col2, col3, col4 = st.columns(4)

if outcomes_df is not None:
    total = len(outcomes_df)
    merged = int((outcomes_df["outcome"] == "merged").sum())

    # Fallback rate only makes sense among runs where a PR was actually
    # opened -- diff_applied is set the moment a PR is created, so runs
    # that never opened one (outcome == "no_pr", low confidence) correctly
    # have no value here and are excluded rather than counted as 0%.
    pr_opened_mask = outcomes_df["diff_applied"].notna()
    prs_opened = int(pr_opened_mask.sum())
    if prs_opened:
        fallback_rate = (outcomes_df.loc[pr_opened_mask, "diff_applied"] == False).mean()
        fallback_display = f"{fallback_rate:.0%}"
    else:
        fallback_display = "—"

    col1.metric("Scored runs (recent)", total)
    col2.metric("PRs opened", prs_opened)
    col3.metric("Merged", merged)
    col4.metric(
        "Fallback rate", fallback_display,
        help="Share of opened PRs where the diff couldn't be applied and a "
             "placeholder fallback commit was used instead of a real fix.",
    )
else:
    for c, label in zip((col1, col2, col3, col4),
                        ("Scored runs (recent)", "PRs opened", "Merged", "Fallback rate")):
        c.metric(label, "—")

st.subheader("Recent scored runs")
if outcomes_df is not None:
    display_df = outcomes_df.copy()
    display_df["outcome"] = display_df["outcome"].replace({
        "opened": "PR open (awaiting review)",
        "no_pr": "No PR (low confidence)",
    })
    st.dataframe(
        display_df[[
            "dag_id", "task_id", "confidence_tier", "confidence_score",
            "outcome", "diff_applied", "pr_number", "updated_at",
        ]],
        use_container_width=True,
    )

st.subheader("Fix history by DAG/task")
history_df = load_fix_history()
if history_df is not None:
    # st.bar_chart on a 2-level MultiIndex renders inconsistently -- collapse
    # dag_id/task_id into one label column instead.
    history_df["dag_task"] = history_df["dag_id"] + " / " + history_df["task_id"]
    st.bar_chart(history_df.set_index("dag_task")["count"])