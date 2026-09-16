"""
Streamlit dashboard -- the live view over BigQuery.

Relationship to dashboard.html: the HTML file is the self-contained
benchmark + weight-tuning view you can open from disk with no credentials
(and the one to use while iterating on weights). This one is the deployed
service that reads what the agent has actually been doing in production.
They share the scoring formula via processor/agent/confidence.py, so a
weight change previewed in one behaves identically in the other.
"""

import json
import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
from google.cloud import bigquery

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "processor"))
from agent import confidence  # noqa: E402

PROJECT = os.environ["GCP_PROJECT"]
DATASET = os.environ.get("BQ_DATASET", "dag_failure_agent")
WEIGHTS_PATH = os.environ.get("CONFIDENCE_WEIGHTS_PATH",
                              str(REPO_ROOT / "config" / "weights.json"))

SIGNAL_COLUMNS = {name: f"s_{name}" for name in confidence.ALL_SIGNALS}

client = bigquery.Client(project=PROJECT)

st.set_page_config(page_title="DAG Failure Agent", layout="wide")
st.title("DAG Failure Agent")


def safe_query(query: str, empty_msg: str) -> Optional[pd.DataFrame]:
    """Runs a query defensively: these tables have repeatedly been missing,
    empty, or mid-migration, and a bare query takes down the whole page
    instead of one panel."""
    try:
        df = client.query(query).to_dataframe()
    except Exception as e:
        st.warning(
            f"Couldn't load this section ({type(e).__name__}). The table may not "
            f"exist yet, or migrations/001_*.sql hasn't been applied."
        )
        return None
    if df.empty:
        st.info(empty_msg)
        return None
    return df


@st.cache_data(ttl=60)
def load_scored_runs(limit: int = 300) -> Optional[pd.DataFrame]:
    # confidence_outcomes is append-only, so one record_id can have an
    # "opened" row and a later "merged"/"rejected" row. Always take the
    # latest row per record_id before joining.
    signal_select = ",\n            ".join(f"s.{c}" for c in SIGNAL_COLUMNS.values())
    query = f"""
        WITH latest_outcome AS (
            SELECT record_id, outcome, pr_number, diff_applied, fallback_reason, updated_at,
                   ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY updated_at DESC) AS rn
            FROM `{PROJECT}.{DATASET}.confidence_outcomes`
        )
        SELECT
            s.record_id, s.dag_id, s.task_id, s.run_id, s.scenario_id,
            s.confidence_score, s.confidence_tier, s.weights_version, s.created_at,
            {signal_select},
            o.outcome, o.pr_number, o.diff_applied, o.fallback_reason, o.updated_at
        FROM `{PROJECT}.{DATASET}.confidence_signals` s
        LEFT JOIN latest_outcome o ON o.record_id = s.record_id AND o.rn = 1
        ORDER BY s.created_at DESC
        LIMIT {int(limit)}
    """
    return safe_query(query, "No scored runs recorded yet.")


@st.cache_data(ttl=60)
def load_fix_history() -> Optional[pd.DataFrame]:
    query = f"""
        SELECT dag_id, task_id, outcome, COUNT(*) AS count
        FROM `{PROJECT}.{DATASET}.fix_history`
        GROUP BY dag_id, task_id, outcome
        ORDER BY count DESC
    """
    return safe_query(query, "No merged/rejected fix history recorded yet.")


def load_weights() -> dict:
    try:
        return confidence.load_weights(WEIGHTS_PATH, strict=True)
    except Exception as e:
        st.warning(f"Falling back to built-in weights: {e}")
        return confidence.normalize_weights_config(confidence.DEFAULT_WEIGHTS)


def signals_from_row(row) -> dict:
    return {name: (None if pd.isna(row.get(col)) else float(row[col]))
            for name, col in SIGNAL_COLUMNS.items()}


df = load_scored_runs()
weights = load_weights()

tab_overview, tab_runs, tab_weights = st.tabs(["Overview", "Runs", "Weights"])

# ---------------------------------------------------------------- Overview
with tab_overview:
    c1, c2, c3, c4, c5 = st.columns(5)
    if df is not None:
        prs = int(df["pr_number"].notna().sum())
        merged = int((df["outcome"] == "merged").sum())
        gated = int((df["outcome"] == "no_pr").sum())

        # Fallback rate only means anything among runs that opened a PR;
        # diff_applied is set when a PR is created, so gated runs correctly
        # have no value and are excluded rather than counted as 0%.
        opened_mask = df["diff_applied"].notna()
        fallback = (f"{(df.loc[opened_mask, 'diff_applied'] == False).mean():.0%}"
                    if opened_mask.any() else "—")

        c1.metric("Scored runs", len(df))
        c2.metric("PRs opened", prs)
        c3.metric("Merged", merged)
        c4.metric("Gated (no PR)", gated,
                  help="Runs the confidence threshold stopped before a PR was opened.")
        c5.metric("Fallback rate", fallback,
                  help="Share of opened PRs where the diff wouldn't apply and a "
                       "placeholder commit was used instead of a real fix.")
    else:
        for col, label in zip((c1, c2, c3, c4, c5),
                              ("Scored runs", "PRs opened", "Merged",
                               "Gated (no PR)", "Fallback rate")):
            col.metric(label, "—")

    st.subheader("Confidence distribution")
    if df is not None and df["confidence_score"].notna().any():
        st.bar_chart(df["confidence_score"].dropna().round(1)
                     .value_counts().sort_index())
        st.caption(f"Current threshold: {weights['confidence_threshold']:.2f}")

    st.subheader("Fix history by DAG/task")
    history = load_fix_history()
    if history is not None:
        history["dag_task"] = history["dag_id"] + " / " + history["task_id"]
        st.bar_chart(history.set_index("dag_task")["count"])

# -------------------------------------------------------------------- Runs
with tab_runs:
    if df is None:
        st.info("Nothing to show yet.")
    else:
        display = df.copy()
        display["outcome"] = display["outcome"].replace({
            "opened": "PR open (awaiting review)",
            "no_pr": "No PR (below threshold)",
        })
        cols = ["dag_id", "task_id", "scenario_id", "confidence_tier",
                "confidence_score", "outcome", "diff_applied", "pr_number",
                "weights_version", "created_at"]
        st.dataframe(display[[c for c in cols if c in display.columns]],
                     use_container_width=True)

        st.subheader("Signal breakdown")
        signal_cols = [c for c in SIGNAL_COLUMNS.values() if c in display.columns]
        if signal_cols and display[signal_cols].notna().any().any():
            st.dataframe(display[["dag_id", "task_id", "confidence_score"] + signal_cols],
                         use_container_width=True)
        else:
            st.info("No per-signal data on these rows. Apply "
                    "migrations/001_confidence_signals_add_signal_columns.sql and "
                    "redeploy the processor -- rows written before that only carry "
                    "the old degenerate s_logs/s_source columns.")

# ----------------------------------------------------------------- Weights
with tab_weights:
    st.subheader("Live weights")
    st.caption(f"Loaded from `{WEIGHTS_PATH}`. Editing here previews only — copy the "
               f"JSON at the bottom into config/weights.json and redeploy the "
               f"processor to make it real.")

    trial = {
        "version": 2,
        "signal_weights": dict(weights["signal_weights"]),
        "penalty_weights": dict(weights["penalty_weights"]),
        "confidence_threshold": weights["confidence_threshold"],
        "tier_thresholds": dict(weights["tier_thresholds"]),
    }

    left, right = st.columns(2)
    with left:
        for name in confidence.POSITIVE_SIGNALS:
            trial["signal_weights"][name] = st.slider(
                name, 0.0, 1.0, float(weights["signal_weights"].get(name, 0.0)), 0.01)
    with right:
        for name in confidence.PENALTY_SIGNALS:
            magnitude = abs(float(weights["penalty_weights"].get(name, 0.0)))
            trial["penalty_weights"][name] = -st.slider(
                f"{name} (penalty)", 0.0, 1.0, magnitude, 0.01)
        trial["confidence_threshold"] = st.slider(
            "confidence_threshold", 0.0, 1.0,
            float(weights["confidence_threshold"]), 0.01)

    if df is not None and any(c in df.columns for c in SIGNAL_COLUMNS.values()):
        scored = []
        for _, row in df.iterrows():
            signals = signals_from_row(row)
            if all(v is None for v in signals.values()):
                continue
            current = confidence.score_signals(signals, weights)
            proposed = confidence.score_signals(signals, trial)
            scored.append({
                "dag_id": row.get("dag_id"),
                "task_id": row.get("task_id"),
                "outcome": row.get("outcome"),
                "current_score": current["score"],
                "current_decision": "PR" if current["meets_threshold"] else "no PR",
                "proposed_score": proposed["score"],
                "proposed_decision": "PR" if proposed["meets_threshold"] else "no PR",
            })

        if scored:
            preview = pd.DataFrame(scored)
            flipped = preview[preview["current_decision"] != preview["proposed_decision"]]
            now_pr = int((preview["current_decision"] == "PR").sum())
            then_pr = int((preview["proposed_decision"] == "PR").sum())

            m1, m2, m3 = st.columns(3)
            m1.metric("Runs re-scored", len(preview))
            m2.metric("PRs under current", now_pr)
            m3.metric("PRs under proposed", then_pr, delta=then_pr - now_pr)

            st.markdown(f"**{len(flipped)} run(s) would change decision.**")
            if len(flipped):
                st.dataframe(flipped, use_container_width=True)

            labeled = df[df["outcome"].isin(["merged", "rejected"])]
            if len(labeled):
                def accuracy(cfg):
                    correct = 0
                    for _, row in labeled.iterrows():
                        result = confidence.score_signals(signals_from_row(row), cfg)
                        correct += (result["meets_threshold"] == (row["outcome"] == "merged"))
                    return correct / len(labeled)

                a1, a2 = st.columns(2)
                a1.metric("Accuracy vs human labels (current)", f"{accuracy(weights):.0%}")
                a2.metric("Accuracy vs human labels (proposed)", f"{accuracy(trial):.0%}")
                st.caption(f"{len(labeled)} human-labeled run(s). Below ~5 per class these "
                           f"numbers swing on a single PR — use "
                           f"`tune_weights.py fit --source fixtures` until then.")
        else:
            st.info("No rows with per-signal data yet.")
    else:
        st.info("Per-signal columns not present yet — apply migrations/001_*.sql.")

    st.subheader("config/weights.json")
    st.code(json.dumps(confidence.normalize_weights_config(trial), indent=2),
            language="json")
