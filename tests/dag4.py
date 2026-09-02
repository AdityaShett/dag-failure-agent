from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def fetch_customer_profile(**context):
    # preferences legitimately absent for guest checkouts — not itself a bug
    profile = {"customer_id": "c_4471", "scores": {"risk": 0.82}, "preferences": None}
    context["ti"].xcom_push(key="profile", value=profile)

def enrich_with_scores(**context):
    profile = context["ti"].xcom_pull(key="profile", task_ids="fetch_customer_profile")
    # --- BUG (intentional): assumes preferences is always a dict, never checks for None ---
    if "marketing_opt_in" in profile["preferences"]:
        profile["scores"]["marketing_eligible"] = True
    context["ti"].xcom_push(key="enriched", value=profile)

def flag_high_risk(**context):
    print("Flagging high-risk customers")

with DAG("dag4", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
    t1 = PythonOperator(task_id="fetch_customer_profile", python_callable=fetch_customer_profile)
    t2 = PythonOperator(task_id="enrich_with_scores", python_callable=enrich_with_scores)
    t3 = PythonOperator(task_id="flag_high_risk", python_callable=flag_high_risk)
    t1 >> t2 >> t3

# Agent RCA Test
# DAG: dag4
# Task: enrich_with_scores_hard
