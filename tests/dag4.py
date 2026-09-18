"""
DAG 4: Import / Dependency Missing

"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.metrics_helper import compute_engagement_score


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
