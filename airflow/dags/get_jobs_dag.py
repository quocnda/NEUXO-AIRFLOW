from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# Nếu dags/ không nằm cùng root project với hook/, plugin/
# thì thêm PYTHONPATH để import được.
# Bạn chỉnh lại BASE_DIR cho đúng cấu trúc deploy của bạn.
BASE_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow")  # ví dụ
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from hook.sqlalchemyHook import SQLAlchemyHook  # noqa: E402
from plugin.Linkedin import Linkedin  # noqa: E402


@dag(
    dag_id="linkedin_get_all_jobs",
    description="Run Linkedin(session).getAllJob() via SQLAlchemyHook session",
    start_date=datetime(2025, 12, 24),
    schedule="0 0 * * *",  # mỗi 24 giờ
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["linkedin", "jobs"],
)
def linkedin_get_all_jobs_dag():
    @task
    def run_job():
        hook = SQLAlchemyHook()
        session = hook.get_session()

        try:
            linkedin = Linkedin(session)
            linkedin.getAllJob()
        except Exception:
            # rollback nếu có transaction
            try:
                session.rollback()
            except Exception:
                pass
            raise
        finally:
            # đóng session cho sạch connection pool
            try:
                session.close()
            except Exception:
                pass

    run_job()


dag = linkedin_get_all_jobs_dag()
