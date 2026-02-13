# hook/sqlalchemyHook.py
from __future__ import annotations

from airflow.models.connection import Connection
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker


class SQLAlchemyHook:
    def __init__(self, conn_id: str = "neuxo_connections"):
        self.conn_id = conn_id
        self._session = None  # để close_session không lỗi

    def get_engine(self):
        # Lấy connection từ secrets backend / metadata DB / env AIRFLOW_CONN_* (tuỳ cấu hình)
        conn = Connection.get_connection_from_secrets(self.conn_id)

        url = URL.create(
            drivername="mysql+pymysql",
            username=conn.login,
            password=conn.password,  # URL.create sẽ encode
            host=conn.host,
            port=conn.port or 3306,
            database=conn.schema,
        )

        return create_engine(url, pool_pre_ping=True)

    def get_session(self):
        engine = self.get_engine()
        SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        self._session = SessionLocal()
        return self._session

    def close_session(self):
        if self._session:
            self._session.close()
            self._session = None
