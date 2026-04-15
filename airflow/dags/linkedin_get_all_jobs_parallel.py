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
from model.model import LinkedinCategory, LinkedinLocation  # noqa: E402
from plugin.Linkedin import Linkedin  # noqa: E402

logger = logging.getLogger(__name__)

MAX_ACTIVE_TASKS = int(os.environ.get("LINKEDIN_MAX_ACTIVE_TASKS", "8"))


def _safe_task_id(text: str, max_len: int = 180) -> str:
	slug = re.sub(r"[^a-zA-Z0-9_]+", "_", text).strip("_").lower()
	return slug[:max_len] if slug else "task"


def _build_request_url(keyword: str, location: str, start: int) -> str:
	return (
		"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
		f"?f_TPR=r86400&keywords={keyword}&location={location}"
		"&origin=JOB_SEARCH_PAGE_SEARCH_BUTTON&refresh=true&position=1&pageNum=0"
		f"&start={start}"
	)


def _fetch_category_location_pairs() -> list[tuple[str, str]]:
	hook = SQLAlchemyHook()
	session = hook.get_session()

	try:
		categories = [
			x[0]
			for x in session.query(LinkedinCategory.name)
			.filter(LinkedinCategory.name.isnot(None))
			.all()
		]
		locations = [
			"United States",
			"India",
			# "Germany",
			"United Kingdom",
			# "Canada",
			# "France",
			# "Netherlands",
			# "Australia",
			# "Singapore",
			# "United Arab Emirates",
			# "Ireland",
			# "Sweden",
			# "Switzerland",
			# "Israel",
			# "China",
			# "Japan",
			# "South Korea",
			# "Spain",
			# "Italy",
			# "Brazil"
		]
	except Exception:
		logger.exception("Failed to load Linkedin categories/locations for DAG")
		return []
	finally:
		try:
			session.close()
		except Exception:
			pass

	return [(c, l) for c in categories for l in locations if l != "Others"]


@dag(
	dag_id="linkedin_get_all_jobs_parallel",
	description="Parallel Linkedin job crawler with manual retry chains",
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
	tags=["linkedin", "jobs", "parallel"],
)
def linkedin_get_all_jobs_parallel_dag():
	@task
	def start():
		return "start"

	@task
	def run_category_location(category: str, location: str) -> None:
		hook = SQLAlchemyHook()
		session = hook.get_session()

		try:
			linkedin = Linkedin(session)

			start_pos = 0
			while True:
				request_url = _build_request_url(category, location, start_pos)
				start_pos, len_response = linkedin.getJobByKeyword(
					request_url,
					category,
					location,
					start_pos,
				)
				logger.info(
					"Keyword: %s, Location: %s, Start: %d, Len: %d",
					category,
					location,
					start_pos,
					len_response,
				)
				if len_response == 0 or start_pos >= 50:
					break

			if category == "Blockchain":
				start_pos = 0
				while True:
					request_url = _build_request_url("web3", location, start_pos)
					start_pos, len_response = linkedin.getJobByKeyword(
						request_url,
						category,
						location,
						start_pos,
					)
					logger.info(
						"Keyword: web3, Location: %s, Start: %d, Len: %d",
						location,
						start_pos,
						len_response,
					)
					if len_response == 0 or start_pos >= 50:
						break

		except Exception:
			try:
				session.rollback()
			except Exception:
				pass
			raise
		finally:
			try:
				session.close()
			except Exception:
				pass

	start_task = start()

	pairs = _fetch_category_location_pairs()
	for category, location in pairs:
		base_id = _safe_task_id(f"jobs_{category}_{location}")

		primary = run_category_location.override(
			task_id=base_id,
			trigger_rule=TriggerRule.ALL_DONE,
		)(category, location)

		retry_1 = run_category_location.override(
			task_id=f"{base_id}_retry_1",
			trigger_rule=TriggerRule.ONE_FAILED,
		)(category, location)

		retry_2 = run_category_location.override(
			task_id=f"{base_id}_retry_2",
			trigger_rule=TriggerRule.ONE_FAILED,
		)(category, location)

		start_task >> primary >> retry_1 >> retry_2


dag = linkedin_get_all_jobs_parallel_dag()
