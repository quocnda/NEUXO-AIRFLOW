from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

# If dags/ is not in the same root with hook/ and plugin/, add project root to PYTHONPATH.
BASE_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow")
if BASE_DIR not in sys.path:
	sys.path.insert(0, BASE_DIR)

from hook.sqlalchemyHook import SQLAlchemyHook  # noqa: E402
from model.model import LinkedinCompany  # noqa: E402
from plugin.Subdomain import Subdomains  # noqa: E402

logger = logging.getLogger(__name__)

MAX_ACTIVE_TASKS = int(os.environ.get("SUBDOMAIN_MAX_ACTIVE_TASKS", "8"))
BATCH_SIZE = int(os.environ.get("SUBDOMAIN_BATCH_SIZE", "20"))
LOOKBACK_DAYS = int(os.environ.get("SUBDOMAIN_LOOKBACK_DAYS", "3"))


def _safe_task_id(text: str, max_len: int = 180) -> str:
	slug = re.sub(r"[^a-zA-Z0-9_]+", "_", text).strip("_").lower()
	return slug[:max_len] if slug else "task"


def _chunk_list(items: list[str], size: int) -> list[list[str]]:
	if size <= 0:
		return [items]
	return [items[i : i + size] for i in range(0, len(items), size)]


def _fetch_recent_company_websites() -> list[str]:
	hook = SQLAlchemyHook()
	session = hook.get_session()

	try:
		cutoff = datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)
		websites = [
			row[0]
			for row in (
				session.query(LinkedinCompany.website)
				.filter(LinkedinCompany.created_at >= cutoff)
				.filter(LinkedinCompany.website.isnot(None))
				.filter(LinkedinCompany.website != "")
				.all()
			)
		]
		return websites
	except Exception:
		logger.exception("Failed to load recent Linkedin companies for subdomain DAG")
		return []
	finally:
		try:
			session.close()
		except Exception:
			pass


@dag(
	dag_id="subdomain_get_recent_parallel",
	description="Fetch subdomains for companies created in the last N days",
	start_date=datetime(2025, 12, 24),
	schedule="0 0 * * *",
	catchup=False,
	max_active_runs=1,
	max_active_tasks=MAX_ACTIVE_TASKS,
	default_args={
		"owner": "airflow",
		"retries": 0,
		"retry_delay": timedelta(minutes=5),
	},
	tags=["subdomain", "linkedin", "parallel"],
)
def subdomain_get_recent_parallel_dag():
	@task
	def start() -> str:
		return "start"

	@task
	def run_batch(websites: list[str]) -> int:
		hook = SQLAlchemyHook()
		session = hook.get_session()
		print('------>>> SESSION :', session)
		total_created = 0

		try:
			for web_url in websites:
				if not web_url:
					continue
				subdomain = Subdomains(session)
				try:
					total_created += subdomain.getSubdomainsByLinkCompany(web_url)
				except Exception:
					logger.exception("Subdomain crawl failed for %s", web_url)

				finally:
					try:
						subdomain.driverQuit()
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

	websites = _fetch_recent_company_websites()
	batches = _chunk_list(websites, BATCH_SIZE)

	for index, batch in enumerate(batches, start=1):
		base_id = _safe_task_id(f"subdomain_batch_{index}")

		primary = run_batch.override(
			task_id=base_id,
			trigger_rule=TriggerRule.ALL_DONE,
		)(batch)

		retry_1 = run_batch.override(
			task_id=f"{base_id}_retry_1",
			trigger_rule=TriggerRule.ONE_FAILED,
		)(batch)

		retry_2 = run_batch.override(
			task_id=f"{base_id}_retry_2",
			trigger_rule=TriggerRule.ONE_FAILED,
		)(batch)

		start_task >> primary >> retry_1 >> retry_2


dag = subdomain_get_recent_parallel_dag()
