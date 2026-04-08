from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

BASE_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow")
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from hook.sqlalchemyHook import SQLAlchemyHook  # noqa: E402
from plugin.Luma import Luma  # noqa: E402


@dag(
    dag_id="luma_get_events",
    description="Run Luma(session).run() via SQLAlchemyHook session",
    start_date=datetime(2025, 12, 24),
    schedule="0 0 * * *",  # mỗi 24 giờ
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["luma", "events"],
)
def luma_get_events_dag():
    @task
    def run_luma():
        hook = SQLAlchemyHook()
        session = hook.get_session()

        try:
            luma = Luma(session)
            luma.run()
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

    run_luma()


dag = luma_get_events_dag()
