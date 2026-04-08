from __future__ import annotations
from typing import Any

from sqlalchemy import or_

from .BaseApify import BaseApify
from model.model import GuestList


class LinkedinEmailScraper(BaseApify):
    DEFAULT_RUN_INPUT = {
        "includePersonalEmails": True,
        "includeWorkEmails": True,
        "linkedinUrls": [
        ],
        "onlyWithEmails": True
    }
    def __init__(self, session):
        ACTOR_NAME = "LINKEDIN_GET_ONLY_EMAIL"
        super().__init__(actor_name=ACTOR_NAME)
        self.session = session

    def run(self, linkedin_urls: list[str]) -> list[dict[str, Any]]:
        run_input = self._default_run_input()
        run_input["linkedinUrls"] = linkedin_urls
        data_guest_email = self.run_actor(run_input=run_input)
        return self.upsert_email_to_guest_list(data_guest_email)


    @classmethod
    def _to_email_list(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            emails = [cls._safe_str(item) for item in value]
            return [item for item in emails if item]
        single = cls._safe_str(value)
        return [single] if single else []

    @classmethod
    def _normalize_linkedin_url(cls, value: Any) -> str | None:
        url = cls._safe_str(value)
        if not url:
            return None
        return url.rstrip("/")

    @classmethod
    def _pick_email(cls, work_email: str | None, personal_emails: list[str]) -> str | None:
        if work_email:
            return work_email
        return personal_emails[0] if personal_emails else None

    def upsert_email_to_guest_list(self, data: list[dict[str, Any]]) -> int:
        session = self.session
        inserted_count = 0

        try:
            for item in data:
                linkedin_url = self._normalize_linkedin_url(
                    item.get("linkedin_url") or item.get("linkedinUrl")
                )
                linkedin_url = linkedin_url.split('.com')[-1].rstrip("/") if linkedin_url else None
                print('--------->>>  LinkedIn URL: ', linkedin_url, ' | Work Email: ', item.get("work_email") or item.get("workEmail"), ' | Personal Emails: ', item.get("personal_emails") or item.get("personalEmails"))
                work_email = self._safe_str(item.get("work_email") or item.get("workEmail"))
                personal_emails = self._to_email_list(
                    item.get("personal_emails") or item.get("personalEmails")
                )

                selected_email = self._pick_email(work_email, personal_emails)

                if linkedin_url and selected_email:
                    session.query(GuestList).filter(
                        or_(
                            GuestList.linkedin_url == linkedin_url,
                            GuestList.linkedin_url == f"{linkedin_url}/",
                        )
                    ).update(
                        {"email": selected_email},
                        synchronize_session=False,
                    )

                inserted_count += 1

            session.commit()
            return inserted_count
        except Exception:
            session.rollback()
            raise
        finally:
            self.session.close()
