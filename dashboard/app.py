import streamlit as st
from google.cloud import bigquery
import pandas as pd
import os

PROJECT = os.environ["GCP_PROJECT"]
DATASET = os.environ["BQ_DATASET"]

client = bigquery.Client(project=PROJECT)

st.set_page_config(page_title="DAG Failure Agent Dashboard", layout="wide")
st.title("DAG Failure Agent — Status Dashboard")

@st.cache_data(ttl=60)
def load_outcomes():
    query = f"""
        SELECT dag_id, task_id, outcome, confidence_score, created_at
        FROM `{PROJECT}.{DATASET}.confidence_outcomes`
        ORDER BY created_at DESC
        LIMIT 200
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=60)
def load_fix_history():
    query = f"""
        SELECT dag_id, task_id, outcome, COUNT(*) as count
        FROM `{PROJECT}.{DATASET}.fix_history`
        GROUP BY dag_id, task_id, outcome
        ORDER BY count DESC
    """
    return client.query(query).to_dataframe()

outcomes_df = load_outcomes()
history_df = load_fix_history()

col1, col2, col3 = st.columns(3)
total = len(outcomes_df)
merged = (outcomes_df["outcome"] == "merged").sum() if total else 0
fallback_rate = (outcomes_df["outcome"] == "fallback").mean() if total else 0

col1.metric("Total PRs (recent)", total)
col2.metric("Merged", merged)
col3.metric("Fallback rate", f"{fallback_rate:.0%}")

st.subheader("Recent PR outcomes")
st.dataframe(outcomes_df, use_container_width=True)

st.subheader("Fix history by DAG/task")
st.bar_chart(history_df.set_index(["dag_id", "task_id"])["count"])