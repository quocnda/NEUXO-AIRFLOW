from __future__ import annotations

import copy
from datetime import datetime, timezone
from typing import Any, ClassVar, Literal

import requests
from apify_client import ApifyClient
from sqlalchemy.exc import SQLAlchemyError

from hook.sqlalchemyHook import SQLAlchemyHook
from model.model import ApifyToken
from . import DEFAULT_MAP_ACTOR_TO_ID

ActorName = Literal[
    "LINKEDIN_GET_LEADS",
    "LINKEDIN_GET_PROFILE_PERSON",
    "LINKEDIN_GET_POST",
    "LINKEDIN_GET_JOB",
    "LINKEDIN_GET_ONLY_EMAIL",
    "LINKEDIN_GET_EMAIL_AND_PROFILE"
]



class BaseApify:
    STATUS_ACTIVE = "ACTIVE"
    STATUS_UNAVAILABLE = "UNAVAILABLE"

    DEFAULT_RUN_INPUT: ClassVar[dict[str, Any]] = {}

    def __init__(
        self,
        actor_name: ActorName | None = None,
    ):
        self.actor_name = actor_name
        self.apify_token = self._select_apify_token()

    @classmethod
    def _default_run_input(cls) -> dict[str, Any]:
        return copy.deepcopy(cls.DEFAULT_RUN_INPUT)

    @staticmethod
    def _safe_str(value: Any) -> str | None:
        if value is None:
            return None
        result = str(value).strip()
        return result or None

    @classmethod
    def _parse_apify_datetime(cls, value: Any) -> datetime | None:
        text = cls._safe_str(value)
        if not text:
            return None

        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None

        if parsed.tzinfo is not None:
            # Store as naive UTC because SQLAlchemy model uses DateTime without tz.
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed

    @classmethod
    def _get_usage_status_from_token(cls, token_value: str) -> tuple[str | None, datetime | None]:
        url = f"https://api.apify.com/v2/users/me/limits?token={token_value}"
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError):
            return None, None

        data = payload.get("data", {})
        limits = data.get("limits", {})
        current = data.get("current", {})
        monthly_cycle = data.get("monthlyUsageCycle", {})

        max_monthly_usage_usd = limits.get("maxMonthlyUsageUsd")
        monthly_usage_usd = current.get("monthlyUsageUsd")
        cycle_end_at = cls._parse_apify_datetime(monthly_cycle.get("endAt"))

        try:
            usage_value = float(monthly_usage_usd)
            limit_value = float(max_monthly_usage_usd)
        except (TypeError, ValueError):
            return None, cycle_end_at

        if usage_value < limit_value:
            return cls.STATUS_ACTIVE, None
        return cls.STATUS_UNAVAILABLE, cycle_end_at

    @classmethod
    def _refresh_apify_token_statuses(cls) -> None:
        hook = SQLAlchemyHook()
        session = hook.get_session()
        now = datetime.utcnow()

        try:
            tokens = (
                session.query(ApifyToken)
                .filter(ApifyToken.token.isnot(None), ApifyToken.token != "")
                .all()
            )

            for token_obj in tokens:
                current_status = (token_obj.status or "").upper()

                if current_status == cls.STATUS_ACTIVE:
                    next_status, next_time_available = cls._get_usage_status_from_token(
                        token_obj.token
                    )
                    if not next_status:
                        continue
                    token_obj.status = next_status
                    token_obj.next_time_available = next_time_available
                    session.commit()
                    continue

                if current_status in {cls.STATUS_UNAVAILABLE, "INACTIVE"}:
                    if (
                        token_obj.next_time_available
                        and token_obj.next_time_available >= now
                    ):
                        next_status, _ = cls._get_usage_status_from_token(token_obj.token)
                        if next_status == cls.STATUS_ACTIVE:
                            token_obj.status = cls.STATUS_ACTIVE
                            token_obj.next_time_available = None
                            session.commit()
        except SQLAlchemyError:
            session.rollback()
            raise
        finally:
            hook.close_session()

    @classmethod
    def _select_apify_token(cls) -> str:
        cls._refresh_apify_token_statuses()

        hook = SQLAlchemyHook()
        session = hook.get_session()
        try:
            active_token = (
                session.query(ApifyToken)
                .filter(
                    ApifyToken.token.isnot(None),
                    ApifyToken.token != "",
                    ApifyToken.status.ilike(cls.STATUS_ACTIVE),
                )
                .order_by(ApifyToken.updated_at, ApifyToken.created_at)
                .first()
            )
        finally:
            hook.close_session()

        if not active_token:
            raise ValueError("No active Apify token available in Apify_Token table")
        return active_token.token

    def run_actor(
        self,
        run_input: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        selected_actor = self.actor_name
        if not selected_actor:
            raise ValueError("actor_name is required")

        actor_id = DEFAULT_MAP_ACTOR_TO_ID.get(selected_actor)
        if not actor_id:
            raise ValueError(f"actor_id not found for actor_name: {selected_actor}")

        if not self.apify_token:
            self.apify_token = self._select_apify_token()

        client = ApifyClient(self.apify_token)
        run = client.actor(actor_id).call(run_input=run_input)
        return list(client.dataset(run["defaultDatasetId"]).iterate_items())