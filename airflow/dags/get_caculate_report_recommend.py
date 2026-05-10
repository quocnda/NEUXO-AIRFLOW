from __future__ import annotations
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

BASE_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow")
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from hook.sqlalchemyHook import SQLAlchemyHook  # noqa: E402
from airflow.dags import USER_RECORDS, USER_IDS
from plugin.algor.recommend_layer import run_online_recommendation_with_scoring

@dag(
    dag_id="get_calculate_report_recommend",
    description="Calculate report recommendations based on user interactions",
    start_date=datetime(2025, 12, 24),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["recommendation", "report"],
)
def get_calculate_report_recommend_dag():
    @task
    def start() -> str: 
        return "Start calculating report recommendations"
    @task
    def calculate_recommendations() -> dict:
        hook = SQLAlchemyHook()
        session = hook.get_session()
        stats: dict = {}

        try:
            stats = run_online_recommendation_with_scoring(user_id=USER_IDS, user_records=USER_RECORDS, session=session)
        except Exception:
            logging.exception("Failed to calculate report recommendations")
        finally:
            try:
                session.rollback()
            except Exception:
                pass

        return stats
    
    start_task = start()
    
    primary = calculate_recommendations.override(
        task_id="calculate_recommendations_primary",
        trigger_rule=TriggerRule.ALL_DONE
    )()
    retry_1 = calculate_recommendations.override(
        task_id="calculate_recommendations_retry_1",
        trigger_rule=TriggerRule.ALL_FAILED
    )()
    retry_2 = calculate_recommendations.override(
        task_id="calculate_recommendations_retry_2",
        trigger_rule=TriggerRule.ALL_FAILED
    )()
    retry_3 = calculate_recommendations.override(
        task_id="calculate_recommendations_retry_3",
        trigger_rule=TriggerRule.ALL_FAILED
    )()
    
    start_task >> primary >> retry_1 >> retry_2 >> retry_3
    
dag = get_calculate_report_recommend_dag()