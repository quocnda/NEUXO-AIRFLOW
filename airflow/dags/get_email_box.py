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
from model.model import MailAppAccount  # noqa: E402
from plugin.Email import EmailPlugin  # noqa: E402

logger = logging.getLogger(__name__)

MAX_ACTIVE_TASKS = int(os.environ.get("EMAIL_MAX_ACTIVE_TASKS", "8"))
BATCH_SIZE = int(os.environ.get("EMAIL_BATCH_SIZE", "30"))


def _safe_task_id(text: str, max_len: int = 180) -> str:
	slug = re.sub(r"[^a-zA-Z0-9_]+", "_", text).strip("_").lower()
	return slug[:max_len] if slug else "task"


def _chunk_list(items: list[str], size: int) -> list[list[str]]:
	if size <= 0:
		return [items]
	return [items[i : i + size] for i in range(0, len(items), size)]


def _fetch_active_mail_account_ids() -> list[str]:
	"""Load ACTIVE mail accounts from DB.

	This runs at DAG parse time (similar to get_subdomain_dag.py).
	"""

	hook = SQLAlchemyHook()
	session = hook.get_session()
	try:
		ids = [
			str(row[0])
			for row in (
				session.query(MailAppAccount.id)
				.filter(MailAppAccount.status == "ACTIVE")
				.all()
			)
			if row and row[0]
		]
		return ids
	except Exception:
		logger.exception("Failed to load ACTIVE MailAppAccount ids")
		return []
	finally:
		try:
			session.close()
		except Exception:
			pass


@dag(
	dag_id="email_crawl_history_daily_batched",
	description="Daily crawl mail history (INBOX + Sent) for ACTIVE mail accounts in batches",
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
	tags=["email", "imap", "history", "batch"],
)
def email_crawl_history_daily_batched_dag():
	@task
	def start() -> str:
		return "start"

	@task
	def run_batch(account_ids: list[str]) -> dict:
		hook = SQLAlchemyHook()
		session = hook.get_session()
		plugin = EmailPlugin(session)

		processed = 0
		errors = 0
		details: list[dict] = []

		try:
			for account_id in account_ids or []:
				account = (
					session.query(MailAppAccount)
					.filter(MailAppAccount.id == account_id)
					.first()
				)
				if not account or account.status != "ACTIVE":
					continue

				try:
					details.append(plugin._crawl_account(account))
					processed += 1
				except Exception as exc:
					errors += 1
					logger.exception("Email crawl failed for account_id=%s", account_id)
					details.append(
						{
							"account_id": str(account_id),
							"email": getattr(account, "email", None),
							"status": "error",
							"error": str(exc),
						}
					)
					try:
						session.rollback()
					except Exception:
						pass

			return {
				"batch_size": len(account_ids or []),
				"processed": processed,
				"errors": errors,
				"details": details,
			}
		finally:
			try:
				session.close()
			except Exception:
				pass

	start_task = start()

	account_ids = _fetch_active_mail_account_ids()
	batches = _chunk_list(account_ids, BATCH_SIZE)
	if not batches or batches == [[]]:
		return

	for index, batch in enumerate(batches, start=1):
		base_id = _safe_task_id(f"email_batch_{index}")

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


dag = email_crawl_history_daily_batched_dag()
