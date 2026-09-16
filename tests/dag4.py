"""
DAG 4: Import / Dependency Missing
Failure mode: ModuleNotFoundError caused by importing from a module path
that was moved during a repo refactor (utils.metrics_helper -> lib.metrics).
Expected agent outcome: PR_CREATED (medium confidence fix).
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.metrics import compute_engagement_score

def score_engagement(**context):
    score = compute_engagement_score(clicks=42, views=310)
    print(f"Engagement score: {score}")


with DAG(
    dag_id="dag4",
    description="Import / Dependency Missing",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["benchmark", "missing-import"],
) as dag:
    score_task = PythonOperator(
        task_id="score_engagement", python_callable=score_engagement
    )
