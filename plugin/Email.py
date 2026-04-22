
from __future__ import annotations

import email
import imaplib
import logging
import os
import re
from datetime import datetime, timedelta, timezone as dt_timezone
from email.header import decode_header
from email.utils import getaddresses, parsedate_to_datetime

from model.model import MailAppAccount, MailHistory


INBOX_FOLDER = "INBOX"
DEFAULT_SENT_CANDIDATES = (
    '"[Gmail]/Sent Mail"',
    '"[Google Mail]/Sent Mail"',
    "Sent",
)

class EmailPlugin:
    def __init__(self, session):
        self.session = session

    def get_mail_accounts(self) -> list[MailAppAccount]:
        return self.session.query(MailAppAccount).all()

    def get_active_mail_accounts(self) -> list[MailAppAccount]:
        return (
            self.session.query(MailAppAccount)
            .filter(MailAppAccount.status == "ACTIVE")
            .all()
        )

    def run_get_email(self) -> dict:
        """Daily job entrypoint.

        - Fetch ACTIVE mail accounts.
        - Crawl today's INBOX + Sent messages if IMAP supports date filtering.
        - Fallback: crawl latest 100 messages per folder.
        - Upsert into MailHistory to avoid duplicates.
        """

        logger = logging.getLogger(__name__)
        accounts = self.get_active_mail_accounts()
        logger.info("Email crawl: found %d ACTIVE account(s)", len(accounts))

        summary: dict = {
            "accounts": len(accounts),
            "processed": 0,
            "errors": 0,
            "details": [],
        }

        for account in accounts:
            try:
                result = self._crawl_account(account)
                summary["processed"] += 1
                summary["details"].append(result)
            except Exception as exc:
                summary["errors"] += 1
                logger.exception(
                    "Email crawl failed for account_id=%s email=%s",
                    getattr(account, "id", None),
                    getattr(account, "email", None),
                )
                summary["details"].append(
                    {
                        "account_id": str(getattr(account, "id", "")),
                        "email": getattr(account, "email", None),
                        "status": "error",
                        "error": str(exc),
                    }
                )
                try:
                    self.session.rollback()
                except Exception:
                    pass

        return summary

    # ------------------------------------------------------------------
    # IMAP crawl helpers
    # ------------------------------------------------------------------

    def _crawl_account(self, account: MailAppAccount) -> dict:
        logger = logging.getLogger(__name__)
        account_email = (account.email or "").strip().lower()
        if not account_email:
            return {
                "account_id": str(account.id),
                "email": account.email,
                "status": "skipped",
                "reason": "missing_email",
            }

        password = (account.password_app or "").strip()
        if not password:
            return {
                "account_id": str(account.id),
                "email": account.email,
                "status": "skipped",
                "reason": "missing_password",
            }

        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        try:
            mail.login(account_email, password)
            sent_folder = self._discover_sent_folder(mail)

            today_uids_inbox = self._search_uids_today(mail, INBOX_FOLDER)
            today_uids_sent = self._search_uids_today(mail, sent_folder)

            if today_uids_inbox is None or today_uids_sent is None:
                logger.warning(
                    "IMAP server does not support date filtering for account_id=%s email=%s. Falling back to latest messages.",
                    account.id,
                    account_email,
                )
            
            used_fallback_inbox = today_uids_inbox is None
            used_fallback_sent = today_uids_sent is None
            

            inbox_uids = (
                self._search_uids_latest(mail, INBOX_FOLDER, limit=100)
                if used_fallback_inbox
                else (today_uids_inbox or [])
            )
            sent_uids = (
                self._search_uids_latest(mail, sent_folder, limit=100)
                if used_fallback_sent
                else (today_uids_sent or [])
            )

            inbox_count = self._fetch_and_upsert(
                mail=mail,
                account=account,
                account_email=account_email,
                folder_name=INBOX_FOLDER,
                uids=inbox_uids,
            )
            sent_count = self._fetch_and_upsert(
                mail=mail,
                account=account,
                account_email=account_email,
                folder_name=sent_folder,
                uids=sent_uids,
            )

            self.session.commit()
            logger.info(
                "Email crawl done for %s (fallback_inbox=%s fallback_sent=%s): inbox=%d sent=%d",
                account_email,
                used_fallback_inbox,
                used_fallback_sent,
                inbox_count,
                sent_count,
            )
            return {
                "account_id": str(account.id),
                "email": account.email,
                "status": "ok",
                "fallback_inbox": used_fallback_inbox,
                "fallback_sent": used_fallback_sent,
                "inbox_count": inbox_count,
                "sent_count": sent_count,
            }
        finally:
            try:
                mail.logout()
            except Exception:
                logger.exception("Failed IMAP logout for %s", account_email)

    def _discover_sent_folder(self, mail: imaplib.IMAP4_SSL) -> str:
        status, folders = mail.list()
        if status != "OK":
            return DEFAULT_SENT_CANDIDATES[0]

        for raw_folder in folders or []:
            folder_text = raw_folder.decode(errors="ignore")
            if "\\Sent" in folder_text:
                match = re.findall(r'"([^"]+)"$', folder_text)
                if match:
                    return f'"{match[-1]}"'

        # Fallback by name match
        for candidate in DEFAULT_SENT_CANDIDATES:
            candidate_name = candidate.replace('"', "").lower()
            for raw_folder in folders or []:
                folder_text = raw_folder.decode(errors="ignore").lower()
                if candidate_name in folder_text:
                    return candidate

        return DEFAULT_SENT_CANDIDATES[0]

    def _search_uids_today(self, mail: imaplib.IMAP4_SSL, folder_name: str) -> list[bytes] | None:
        """Return list of UIDs for today's messages in `folder_name`.

        Returns None when date filtering is not supported or search/select fails.
        """

        status, _ = mail.select(folder_name, readonly=True)
        if status != "OK":
            return None

        today = datetime.now(dt_timezone.utc).date()
        tomorrow = today + timedelta(days=1)
        since_str = today.strftime("%d-%b-%Y")
        before_str = tomorrow.strftime("%d-%b-%Y")

        try:
            status, data = mail.search(None, "SINCE", since_str, "BEFORE", before_str)
        except Exception:
            return None

        if status != "OK":
            return None

        uids = (data[0] or b"").split()
        # Newest first
        return list(reversed(uids))

    def _search_uids_latest(
        self,
        mail: imaplib.IMAP4_SSL,
        folder_name: str,
        limit: int,
    ) -> list[bytes]:
        status, _ = mail.select(folder_name, readonly=True)
        if status != "OK":
            return []

        status, data = mail.search(None, "ALL")
        if status != "OK":
            return []

        uids = list(reversed((data[0] or b"").split()))
        return uids[: max(int(limit), 0)]

    def _fetch_and_upsert(
        self,
        mail: imaplib.IMAP4_SSL,
        account: MailAppAccount,
        account_email: str,
        folder_name: str,
        uids: list[bytes],
    ) -> int:
        logger = logging.getLogger(__name__)
        if not uids:
            return 0

        status, _ = mail.select(folder_name, readonly=True)
        if status != "OK":
            return 0

        processed = 0
        seen_message_ids: set[str] = set()

        for uid in uids:
            fetch_status, msg_data = mail.fetch(uid, "(RFC822)")
            if fetch_status != "OK":
                continue

            raw_bytes: bytes | None = None
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    raw_bytes = response_part[1]
                    break
            if not raw_bytes:
                continue

            try:
                message = email.message_from_bytes(raw_bytes)
                payload, body_text, body_html, in_reply_to = self._build_mail_history_payload(
                    account=account,
                    account_email=account_email,
                    message=message,
                    folder_name=folder_name,
                    uid=uid,
                )

                message_id = payload.get("message_id") or ""
                if message_id and message_id in seen_message_ids:
                    continue
                if message_id:
                    seen_message_ids.add(message_id)

                self._upsert_mail_history(payload)

                # Bounce handling: update original message by reply-id
                self._mark_bounced(
                    payload=payload,
                    reply_id=in_reply_to,
                    body_text=body_text,
                    body_html=body_html,
                )

                processed += 1
            except Exception:
                logger.exception(
                    "Failed to upsert message uid=%s folder=%s account=%s",
                    uid.decode(errors="ignore"),
                    folder_name,
                    account_email,
                )
                continue

        return processed

    # ------------------------------------------------------------------
    # Parse + DB upsert
    # ------------------------------------------------------------------

    def _decode_header_value(self, value: str | None) -> str:
        if not value:
            return ""

        decoded_parts: list[str] = []
        for part, encoding in decode_header(value):
            if isinstance(part, bytes):
                decoded_parts.append(part.decode(encoding or "utf-8", errors="ignore"))
            else:
                decoded_parts.append(str(part))
        return "".join(decoded_parts).strip()

    def _parse_email_date(self, value: str | None) -> datetime:
        if not value:
            return datetime.now(dt_timezone.utc).replace(tzinfo=None)

        try:
            parsed = parsedate_to_datetime(value)
            if parsed is None:
                return datetime.now(dt_timezone.utc).replace(tzinfo=None)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt_timezone.utc)
            return parsed.astimezone(dt_timezone.utc).replace(tzinfo=None)
        except Exception:
            return datetime.now(dt_timezone.utc).replace(tzinfo=None)

    def _extract_addresses(self, header_value: str | None) -> list[str]:
        addresses: list[str] = []
        for _, addr in getaddresses([header_value or ""]):
            if addr:
                addresses.append(addr.lower().strip())
        return addresses

    def _strip_html(self, html: str) -> str:
        if not html:
            return ""
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _extract_message_bodies(self, message: email.message.Message) -> tuple[str, str]:
        text_body = ""
        html_body = ""

        if message.is_multipart():
            for part in message.walk():
                content_type = part.get_content_type()
                disposition = str(part.get("Content-Disposition", ""))
                if "attachment" in disposition.lower():
                    continue

                payload = part.get_payload(decode=True)
                if not payload:
                    continue

                charset = part.get_content_charset() or "utf-8"
                try:
                    decoded = payload.decode(charset, errors="ignore")
                except Exception:
                    decoded = payload.decode("utf-8", errors="ignore")

                if content_type == "text/plain" and not text_body:
                    text_body = decoded
                elif content_type == "text/html" and not html_body:
                    html_body = decoded
        else:
            payload = message.get_payload(decode=True)
            if payload:
                charset = message.get_content_charset() or "utf-8"
                try:
                    decoded = payload.decode(charset, errors="ignore")
                except Exception:
                    decoded = payload.decode("utf-8", errors="ignore")

                if message.get_content_type() == "text/html":
                    html_body = decoded
                else:
                    text_body = decoded

        if not text_body and html_body:
            text_body = self._strip_html(html_body)

        return text_body.strip(), html_body.strip()

    def _build_mail_history_payload(
        self,
        account: MailAppAccount,
        account_email: str,
        message: email.message.Message,
        folder_name: str,
        uid: bytes,
    ) -> tuple[dict, str, str, str]:
        from_header = self._decode_header_value(message.get("From"))
        to_header = self._decode_header_value(message.get("To"))
        subject = self._decode_header_value(message.get("Subject"))

        message_id_header = (message.get("Message-ID") or "").strip()
        in_reply_to = (message.get("In-Reply-To") or "").strip()
        references = (message.get("References") or "").strip()

        body_text, body_html = self._extract_message_bodies(message)
        sent_at = self._parse_email_date(message.get("Date"))

        from_addresses = self._extract_addresses(from_header)
        to_addresses = self._extract_addresses(to_header)
        is_sent = any(addr == account_email for addr in from_addresses)

        if is_sent:
            main_target_mail = next(
                (addr for addr in to_addresses if addr != account_email), ""
            )
            mail_type = "SEND"
            mail_send = account_email
            mail_received = ", ".join(to_addresses)
        else:
            main_target_mail = next(
                (addr for addr in from_addresses if addr != account_email), ""
            )
            mail_type = "RECIEVE"
            mail_send = ", ".join(from_addresses)
            mail_received = ", ".join(to_addresses) or account_email

        if not main_target_mail:
            combined = to_addresses if is_sent else from_addresses
            main_target_mail = next((addr for addr in combined if addr), "")

        durable_message_id = (
            message_id_header
            or f"{folder_name}:{uid.decode(errors='ignore')}:{account.id}"
        )
        first_reference = ""
        if references:
            first_reference = references.split()[0].strip()

        payload = {
            "user_id": account.user_id,
            "mail_send": mail_send,
            "mail_recieved": mail_received,
            "content": (body_text or "")[:10000],
            "html_mail_content": (body_html or "")[:50000],
            "time_send": sent_at,
            "subject": (subject or "")[:1000],
            "main_target_mail": main_target_mail,
            "name_target_mail": main_target_mail,
            "type": mail_type,
            "message_id": durable_message_id,
            "status_mail": "SUCCESS",
            "email_ref_first_id": in_reply_to or first_reference or durable_message_id,
            "email_reply_id": in_reply_to or first_reference,
        }

        return payload, body_text, body_html, in_reply_to

    def _upsert_mail_history(self, payload: dict) -> None:
        """Upsert MailHistory by (user_id, message_id) to avoid duplicates."""

        message_id = payload.get("message_id")
        if not message_id:
            return

        existing = (
            self.session.query(MailHistory)
            .filter(
                MailHistory.user_id == payload.get("user_id"),
                MailHistory.message_id == message_id,
            )
            .first()
        )

        if existing:
            for key, value in payload.items():
                if hasattr(existing, key):
                    setattr(existing, key, value)
        else:
            record = MailHistory(**{k: v for k, v in payload.items() if hasattr(MailHistory, k)})
            self.session.add(record)

        self.session.flush()

    def _mark_bounced(self, payload: dict, reply_id: str, body_text: str, body_html: str) -> int:
        """If this message is from mailer-daemon, mark original message as ERROR.

        Logic mirrors legacy `SendEmail.py`: find emails in bounce body and update
        MailHistory rows with message_id == reply_id and main_target_mail in extracted emails.
        """

        main_target_mail = (payload.get("main_target_mail") or "").lower().strip()
        if main_target_mail != "mailer-daemon@googlemail.com":
            return 0

        normalized_reply = (reply_id or "").strip()
        if not normalized_reply:
            return 0

        error_message = (body_text or body_html or "").strip()
        if not error_message:
            return 0

        emails = {
            email_address.lower().strip()
            for email_address in re.findall(
                r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
                error_message,
            )
            if email_address
        }
        emails.discard("mailer-daemon@googlemail.com")
        if not emails:
            return 0

        updated = (
            self.session.query(MailHistory)
            .filter(
                MailHistory.message_id == normalized_reply,
                MailHistory.main_target_mail.in_(sorted(emails)),
            )
            .update(
                {
                    MailHistory.status_mail: "ERROR",
                    MailHistory.error_message: error_message[:10000],
                    MailHistory.updated_at: datetime.now(dt_timezone.utc).replace(tzinfo=None),
                },
                synchronize_session=False,
            )
        )
        if updated:
            self.session.flush()
        return int(updated or 0)