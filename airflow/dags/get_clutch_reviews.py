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
from plugin.Clutch import ClutchCrawler  # noqa: E402

logger = logging.getLogger(__name__)

@dag(
    dag_id="get_clutch_reviews",
    description="Fetch reviews from Clutch for companies in the database",
    start_date=datetime(2025, 12, 24),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["clutch", "reviews"],
)
def get_clutch_reviews_dag():
    @task
    def start() -> str:
        return "start"

    @task
    def run_clutch_reviews(batch_start: int, batch_end: int) -> dict:
        hook = SQLAlchemyHook()
        session = hook.get_session()
        total_stats: dict = {}

        try:
            clutch_crawler = ClutchCrawler(session)
            try:
                total_stats = clutch_crawler.run(start_page=batch_start, end_page=batch_end)
            except Exception:
                logger.exception("Clutch review crawl failed")
            finally:
                try:
                    clutch_crawler.driverQuit()
                except Exception:
                    pass
            
            try:
                session.rollback()
            except Exception:
                pass

        except Exception:
            logger.exception("Failed to get database session")

        return total_stats

    start_task = start()

    batches = [
        (1, 3),
        (4, 6),
        (7, 10),
    ]

    previous_tail = start_task
    for index, (batch_start, batch_end) in enumerate(batches, start=1):
        primary = run_clutch_reviews.override(
            task_id=f"run_clutch_reviews_batch_{index}_primary",
            trigger_rule=TriggerRule.ALL_DONE,
        )(batch_start=batch_start, batch_end=batch_end)

        retry_1 = run_clutch_reviews.override(
            task_id=f"run_clutch_reviews_batch_{index}_retry_1",
            trigger_rule=TriggerRule.ONE_FAILED,
        )(batch_start=batch_start, batch_end=batch_end)

        retry_2 = run_clutch_reviews.override(
            task_id=f"run_clutch_reviews_batch_{index}_retry_2",
            trigger_rule=TriggerRule.ONE_FAILED,
        )(batch_start=batch_start, batch_end=batch_end)

        retry_3 = run_clutch_reviews.override(
            task_id=f"run_clutch_reviews_batch_{index}_retry_3",
            trigger_rule=TriggerRule.ONE_FAILED,
        )(batch_start=batch_start, batch_end=batch_end)

        previous_tail >> primary >> retry_1 >> retry_2 >> retry_3
        previous_tail = retry_3
        
dag = get_clutch_reviews_dag()
