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
 
from hook.sqlalchemyHook import SQLAlchemyHook  
from plugin.CoinCarpFunding import CoinCarpScraper

logger = logging.getLogger(__name__)

@dag(
    dag_id="get_funding_coincarp",
    description="Fetch funding data from CoinCarp",
    start_date=datetime(2025, 12, 24),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["funding", "coincarp"],
)
def get_funding_coincarp_dag():
    @task
    def start() -> str:
        return "start"

    @task
    def run_funding(batch_start: int, batch_end: int) -> int:
        hook = SQLAlchemyHook()
        session = hook.get_session()
        total_fundings = 0

        try:
            scraper = CoinCarpScraper(session)
            try:
                page = batch_start
                while page <= batch_end:
                    scraper.insert_data(page=page)
                    page += 1
            except Exception:
                logger.exception("Funding data crawl failed")
            finally:
                try:
                    scraper.close()
                except Exception:
                    pass
            
            try:
                session.rollback()
            except Exception:
                pass

        except Exception:
            logger.exception("Database session error")
        finally:
            try:
                session.close()
            except Exception:
                pass
        
        return total_fundings

    start_task = start()

    batches = [
        (1, 3),
        (4, 6),
        (7, 10),
    ]

    previous_tail = start_task
    for index, (batch_start, batch_end) in enumerate(batches, start=1):
        primary = run_funding.override(
            task_id=f"run_funding_batch_{index}_primary",
            trigger_rule=TriggerRule.ALL_DONE,
        )(batch_start=batch_start, batch_end=batch_end)

        retry_1 = run_funding.override(
            task_id=f"run_funding_batch_{index}_retry_1",
            trigger_rule=TriggerRule.ONE_FAILED,
        )(batch_start=batch_start, batch_end=batch_end)

        retry_2 = run_funding.override(
            task_id=f"run_funding_batch_{index}_retry_2",
            trigger_rule=TriggerRule.ONE_FAILED,
        )(batch_start=batch_start, batch_end=batch_end)

        retry_3 = run_funding.override(
            task_id=f"run_funding_batch_{index}_retry_3",
            trigger_rule=TriggerRule.ONE_FAILED,
        )(batch_start=batch_start, batch_end=batch_end)

        previous_tail >> primary >> retry_1 >> retry_2 >> retry_3
        previous_tail = retry_3