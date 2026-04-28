from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

# If dags/ is not in the same root with hook/ and plugin/, add project root to PYTHONPATH.
BASE_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow")
if BASE_DIR not in sys.path:
	sys.path.insert(0, BASE_DIR)

from hook.sqlalchemyHook import SQLAlchemyHook  # noqa: E402
from plugin.News import NewsPlugin 

logger = logging.getLogger(__name__)

@dag(
	dag_id="get_news_recent",
	description="Fetch news for companies created in the last N days",
	start_date=datetime(2025, 12, 24),
	schedule="0 1 * * *",
	catchup=False,
	max_active_runs=1,
	default_args={
		"owner": "airflow",
		"retries": 0,
		"retry_delay": timedelta(minutes=5),
	},
	tags=["news", "linkedin"],
)
def get_news_recent_dag():
	@task
	def start() -> str:
		return "start"

	@task
	def run_news() -> int:
		hook = SQLAlchemyHook()
		session = hook.get_session()
		total_created = 0

		try:
			news_crawler = NewsPlugin(session)
			try:
				# Run news crawling for all companies in one batch
				total_created = news_crawler.get_techcrunch_news()
			except Exception:
				logger.exception("News crawl failed")
			finally:
				try:
					news_crawler.driverQuit()
				except Exception:
					pass
			
			try:
				session.rollback()
			except Exception:
				pass
		finally:
			try:
				session.close()
			except Exception:
				pass

		return total_created

	start_task = start()


	# Single batch with 3 retries
	primary = run_news.override(
		task_id="run_news_primary",
		trigger_rule=TriggerRule.ALL_DONE,
	)()

	retry_1 = run_news.override(
		task_id="run_news_retry_1",
		trigger_rule=TriggerRule.ONE_FAILED,
	)()

	retry_2 = run_news.override(
		task_id="run_news_retry_2",
		trigger_rule=TriggerRule.ONE_FAILED,
	)()

	retry_3 = run_news.override(
		task_id="run_news_retry_3",
		trigger_rule=TriggerRule.ONE_FAILED,
	)()

	start_task >> primary >> retry_1 >> retry_2 >> retry_3


dag = get_news_recent_dag()
