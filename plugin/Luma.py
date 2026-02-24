from __future__ import annotations

import logging
import os
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC    
import requests
import re
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import or_
from sqlalchemy.orm import joinedload

from model.model import EventsList, GuestList, MainEvents, LinkedinCompany, Notification
from plugin.Linkedin import getCompanyInfor
from plugin.utils.format_data import updateLinkedinUrl
from webdriver_manager.chrome import ChromeDriverManager
from typing import Dict
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from plugin.Apollo  import getCompanyByUrl
# Nếu bạn bật lại GenAI thì uncomment + import đúng module
# from genAI.general_agent import SummaryCompanyDescription, genLabelCompany

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def parse_iso_datetime(value):
    """Parse ISO 8601 datetime string to Python datetime object."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        # Handle ISO 8601 format: '2026-02-12T03:00:00.000Z'
        try:
            # Remove 'Z' and parse
            if value.endswith('Z'):
                value = value[:-1] + '+00:00'
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            try:
                # Fallback: try without timezone
                return datetime.fromisoformat(value.replace('Z', '').replace('.000', ''))
            except ValueError:
                return None
    return None


PASSWORD_FILE = f"{BASE_DIR}/credentials/auth_login_luma.json"
COOKIES_FILE = f"{BASE_DIR}/credentials/cookies_luma.json"
class LumaAuthManager:
    def __init__(self):
        self.account_credentials = {}
        self.all_cookies = {}
        
        with open(PASSWORD_FILE) as f:
            self.account_credentials = json.load(f)

        if os.path.exists(COOKIES_FILE):
            with open(COOKIES_FILE) as f:
                self.all_cookies = json.load(f)
        else:
            for key in self.account_credentials:
                print(f"Initial cookie for {key}")
                password = self.account_credentials[key]["password"]
                cookies = self.login_and_get_cookies(key, password)
                self.all_cookies[key] = cookies
            self.save_cookies()

    def get_cookies(self, email: str) -> dict:
        return self.all_cookies.get(email, {})

    def refresh_cookie(self, email: str):
        print(f"Refreshing cookie for {email}")
        try:
            password = self.account_credentials[email]["password"]
            cookies = self.login_and_get_cookies(email, password)
            self.all_cookies[email] = cookies
            self.save_cookies()
            return cookies
        except Exception as e:
            print(f"Error when get cookie for {email}: {e}")
            return {}

    def refresh_all(self):
        for email in self.account_credentials:
            self.refresh_cookie(email)

    def save_cookies(self):
        with open(COOKIES_FILE, "w") as f:
            json.dump(self.all_cookies, f, indent=2)

    def login_and_get_cookies(self, email: str, password: str) -> Dict[str, str]:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1280,800")
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        )

        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        driver = None
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)

            wait = WebDriverWait(driver, 30)

            driver.get("https://lu.ma/signin")

            wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, 'input.lux-input[type="email"]'))
            ).send_keys(email)

            for btn in driver.find_elements(By.CSS_SELECTOR, "button.lux-button"):
                if "Continue with Email" in btn.text:
                    btn.click()
                    break

            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'input.lux-input[type="password"]'))).send_keys(password)

            for btn in driver.find_elements(By.CSS_SELECTOR, "button.lux-button"):
                if btn.text.strip() == "Continue" or "Continue" in btn.text:
                    btn.click()
                    break

            try:
                wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//div[text()='Not Now']]"))).click()
            except TimeoutException:
                pass

            wait.until(EC.url_contains("/home"))

            cookie_dict = {c["name"]: c["value"] for c in driver.get_cookies()}

            return {
                "luma.first-page": cookie_dict.get("luma.first-page", ""),
                "luma.auth-session-key": cookie_dict.get("luma.auth-session-key", ""),
                "luma.did": cookie_dict.get("luma.did", ""),
            }

        except (TimeoutException, WebDriverException) as e:
            raise RuntimeError(f"Luma login failed: {e}") from e

        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

class Luma:
    def __init__(self, session):
        """
        session: SQLAlchemy Session (vd: airflow SQLAlchemyHook.get_session() trả về Session)
        """
        self.session = session
        # self.endpoints = "https://api.lu.ma/" ##version 1
        self.endpoints = "https://api2.luma.com/"
        self.auth_manager = LumaAuthManager()
        self.all_cookies = self.auth_manager.all_cookies
    # ---------------------------------------------------------------------
    # DB helpers (SQLAlchemy)
    # ---------------------------------------------------------------------
    def _update_or_create_event(self, api_id: str, defaults: dict):
        try:
            event = self.session.query(EventsList).filter(EventsList.api_id == api_id).first()
            if event:
                for key, value in defaults.items():
                    setattr(event, key, value)
                self.session.flush()
                return event, False
            else:
                event = EventsList(api_id=api_id, **defaults)
                self.session.add(event)
                self.session.flush()
                return event, True
        except Exception as e:
            import traceback
            traceback.print_exc()

    def _update_or_create_main_event(self, api_id: str, defaults: dict):
        """SQLAlchemy version of update_or_create for MainEvents"""
        main_event = self.session.query(MainEvents).filter(MainEvents.api_id == api_id).first()
        if main_event:
            for key, value in defaults.items():
                setattr(main_event, key, value)
            self.session.flush()
            return main_event, False
        else:
            main_event = MainEvents(api_id=api_id, **defaults)
            self.session.add(main_event)
            self.session.flush()
            return main_event, True

    def _update_or_create_company(self, linkedin_url: str, defaults: dict):
        """SQLAlchemy version of update_or_create for LinkedinCompany"""
        company = self.session.query(LinkedinCompany).filter(
            LinkedinCompany.linkedin_url == linkedin_url
        ).first()
        if company:
            for key, value in defaults.items():
                setattr(company, key, value)
            self.session.flush()
            return company, False
        else:
            company = LinkedinCompany(linkedin_url=linkedin_url, **defaults)
            self.session.add(company)
            self.session.flush()
            return company, True

    def _get_or_create_notification(self, reference_id, type_, company, defaults: dict):
        """SQLAlchemy version of get_or_create for Notification"""
        existed = (
            self.session.query(Notification)
            .filter(
                Notification.reference_id == str(reference_id),
                Notification.type == type_,
                Notification.company_id == (company.id if company else None),
            )
            .first()
        )
        if existed:
            return existed, False

        n = Notification(
            reference_id=str(reference_id),
            type=type_,
            company_id=(company.id if company else None),
            title=defaults.get("title"),
            post_url=defaults.get("post_url"),
            time_post=defaults.get("time_post"),
        )
        self.session.add(n)
        self.session.flush()
        return n, True

    # ---------------------------------------------------------------------
    # Business logic
    # ---------------------------------------------------------------------

    def run(self):
        for account in self.all_cookies:
            try:
                print(f"        -----> Getting events for {account}")
                self.account = account
                self.cookies = self.auth_manager.get_cookies(account)

                try:
                    self.getEventList(type_data_get="future")
                    self.getEventList(type_data_get="past")
                    self.getAllGuests()
                    self.getMainEvents()
                    self.session.commit()
                except Exception as inner_e:
                    print(f"Error when get data with {account}: {inner_e}")
                    print(f"Refresh cookie for {account}")
                    self.session.rollback()

                    self.cookies = self.auth_manager.refresh_cookie(account)

                    try:
                        self.getEventList(type_data_get="future")
                        self.getEventList(type_data_get="past")
                        self.getAllGuests()
                        self.getMainEvents()
                        self.session.commit()
                    except Exception as retry_e:
                        print(f"Error retry for {account}: {retry_e}")
                        self.session.rollback()
                        continue

            except Exception as e:
                print(f"{account} error: ", e)
                continue

        print("         -----> Getting all emails")
        self.getEmailAllGuest()
        self.getMainEvents()

    def getEventList(self, type_data_get="future"):
        lst_event = []
        next_pagination, has_more = "", None
        while True:
            if has_more is None:
                link = f"{self.endpoints}home/get-events?period={type_data_get}&pagination_limit=25"
            else:
                link = f"{self.endpoints}home/get-events?period={type_data_get}&pagination_cursor={next_pagination}&pagination_limit=25"
            rsp = requests.get(link, cookies=self.cookies).json()
            # time.sleep(1)
            for event in rsp["entries"]:
                try:
                    event["event"]["ticket_key"] = event["role"]["ticket_key"]
                    event["event"]["approval_status"] = event["role"]["approval_status"]
                    event["event"]["guest_count"] = event["guest_count"]
                    event["event"]["start_at"] = event["start_at"]
                    lst_event.append(event["event"])
                except (KeyError, TypeError) as e:
                    print(f"Ignore an event due to missing data: {e}")
                    continue
            has_more = rsp["has_more"]
            if not has_more:
                break
            next_pagination = rsp["next_cursor"]

        for event in lst_event:
            cleaned_event = {key: re.sub(r"[\x00-\x1F]+", "", value) if isinstance(value, str) else value for key, value in event.items()}
            geo_address_info = cleaned_event.get("geo_address_info", {})
            if geo_address_info is None:
                country, full_address = None, None
            else:
                country = geo_address_info.get("country", None)
                full_address = geo_address_info.get("address", None)
                if full_address is None:
                    full_address = geo_address_info.get("city", None)
                if full_address is None:
                    full_address = geo_address_info.get("address", None)

            event_class, created = self._update_or_create_event(
                api_id=cleaned_event["api_id"],
                defaults={
                    "name": cleaned_event["name"],
                    "event_url": cleaned_event["url"],
                    "start_date": parse_iso_datetime(cleaned_event["start_at"]),
                    "ticket_key": cleaned_event["ticket_key"],
                    "account": self.account,
                    "country": country,
                    "location": full_address,
                    "event_image": cleaned_event["cover_url"],
                    "approval_status": cleaned_event["approval_status"],
                    "guest_count": cleaned_event["guest_count"],
                    "start_at": parse_iso_datetime(cleaned_event["start_at"]),
                    "end_at": parse_iso_datetime(cleaned_event["end_at"]),
                    "updated_at": datetime.now(),
                },
            )
            if created:
                print(f"        -----> Added: {cleaned_event['name']}")
                self.getMainEvent(event_class)

    def getMainEvent(self, event: EventsList):
        if event.event_parent_path is not None:
            return
        rsp = requests.get(f"{self.endpoints}event/get?event_api_id={event.api_id}", cookies=self.cookies).json()
        if "featured_infos" not in rsp or len(rsp["featured_infos"]) == 0:
            event.event_parent = "No main event"
            event.updated_at = datetime.now()
            self.session.flush()
            return

        event.event_parent = rsp["featured_infos"][0].get("name", None)
        event.event_parent_path = rsp["featured_infos"][0].get("path", None)
        event.updated_at = datetime.now()
        self.session.flush()

    def getMainEvents(self):
        date_from = datetime.now().date() - timedelta(days=7)
        date_to = datetime.now().date() + timedelta(days=50)
        
        all_main_events = (
            self.session.query(EventsList)
            .filter(
                EventsList.event_parent_path.is_(None),
                EventsList.account == self.account,
                EventsList.start_date >= date_from,
                EventsList.start_date <= date_to,
            )
            .all()
        )
        for event in all_main_events:
            # print(f"        -----> Getting main event for {event.name}")
            self.getMainEvent(event)

        # Get distinct event_parent_path for events without main_event
        all_main_events = (
            self.session.query(EventsList.event_parent, EventsList.event_parent_path)
            .filter(
                EventsList.event_parent_path.isnot(None),
                EventsList.account == self.account,
                EventsList.main_event_id.is_(None),
                EventsList.start_date >= date_from,
                EventsList.start_date <= date_to,
            )
            .distinct()
            .all()
        )
        for main_event_row in all_main_events:
            # print(f"        -----> Getting main event for {main_event_row.event_parent} - {main_event_row.event_parent_path}")

            main_event_path_raw = main_event_row.event_parent_path
            main_event_path = main_event_path_raw.replace("?k=c", "").replace("/", "").replace("?k=p", "")
            url = f"{self.endpoints}url?url={main_event_path}"
            rsp = requests.get(url, cookies=self.cookies).json()
            if "data" in rsp and "calendar" in rsp["data"]:
                rsp = rsp["data"]["calendar"]

                event_class, created = self._update_or_create_main_event(
                    api_id=rsp["api_id"],
                    defaults={
                        "name": rsp["name"],
                        "description": rsp["description_short"],
                        "event_url": "https://lu.ma/" + main_event_path,
                        "cover_image_url": rsp["cover_image_url"],
                        "avatar_image_url": rsp["avatar_url"],
                        "geo_city": rsp["geo_city"],
                        "geo_country": rsp["geo_country"],
                        "geo_region": rsp["geo_region"],
                        "instagram_url": rsp["instagram_handle"],
                        "linkedin_url": rsp["linkedin_handle"],
                        "twitter_url": rsp["twitter_handle"],
                        "website": rsp["website"],
                        "verified_at": parse_iso_datetime(rsp["verified_at"]),
                        "updated_at": datetime.now(),
                    },
                )
            else:
                rsp = rsp["data"]["place"]
                event_class, created = self._update_or_create_main_event(
                    api_id=rsp["api_id"],
                    defaults={
                        "name": rsp["name"],
                        "description": rsp["description"],
                        "event_url": "https://lu.ma/" + main_event_path,
                        "cover_image_url": rsp["hero_image_desktop_url"],
                        "avatar_image_url": rsp["icon_url"],
                        "geo_city": rsp["name"],
                        "updated_at": datetime.now(),
                    },
                )

            # Update all EventsList with this event_parent_path
            self.session.query(EventsList).filter(
                EventsList.event_parent_path == main_event_path_raw
            ).update({"main_event_id": event_class.id})
            self.session.flush()

    def findCompany(self, linkedin_url):
        linkedin_url = "https://www.linkedin.com" + linkedin_url
        if "/company/" in linkedin_url or "/school/" in linkedin_url:
            return None
        
        # TODO: Implement getCompanyByUrl function
        # company_data = getCompanyByUrl(linkedin_url, type="linkedin_profile_url")
        # if company_data is None:
        #     return None
        # For now, just get company info from LinkedIn directly
        company = None
        detail_info_company = getCompanyInfor(linkCompany=linkedin_url)
        if detail_info_company == {}:
            return None

        defaults = {
            "name": detail_info_company.get("name"),
            "website": detail_info_company.get("website"),
            "description": detail_info_company.get("description"),
            "industry": detail_info_company.get("industry"),
            "organization_type": detail_info_company.get("organization_type"),
            "headquarters": detail_info_company.get("headquarters"),
            "followers": detail_info_company.get("followers"),
            "size": detail_info_company.get("size"),
            "avatar_url": detail_info_company.get("avatar_url"),
        }

        # Nếu dùng GenAI thì bật lại:
        # generate_short_description_company = SummaryCompanyDescription().generate(defaults["description"], defaults["industry"])
        # if generate_short_description_company is not None:
        #     defaults["short_description"] = generate_short_description_company
        #
        # label, category = genLabelCompany().generate(defaults["description"], defaults["industry"])
        # if category and (category != "Waiting"):
        #     defaults["category"] = category
        #     defaults["labels"] = label

        company, created = self._update_or_create_company(
            linkedin_url=updateLinkedinUrl(linkedin_url),
            defaults=defaults,
        )
        return company

    def find_company_by_domain(self, domain):
        if not domain:
            return None
        
        # TODO: Implement getCompanyByUrl function for domain lookup
        # company_data = getCompanyByUrl(domain, type="domain")
        # if company_data is None:
        #     return None
        # For now, return None since we don't have the external API
        return None

    def getGuestList(self, event: EventsList):
        lst_guest = []
        next_pagination, has_more = "", None
        count_adding, count_existed = 0, 0
        print('---------> Getting guest list for event:', event.name)
        while True:
            if has_more is None:
                link = f"{self.endpoints}event/get-guest-list?event_api_id={event.api_id}&ticket_key={event.ticket_key}&pagination_limit=100"
            else:
                link = f"{self.endpoints}event/get-guest-list?event_api_id={event.api_id}&ticket_key={event.ticket_key}&pagination_cursor={next_pagination}&pagination_limit=100"
            rsp = requests.get(link, cookies=self.cookies).json()
            if "message" in rsp and "You do not have access to see the guest list." in rsp["message"]:
                return
            if "entries" not in rsp:
                return

            lst_guest += rsp["entries"]
            has_more = rsp["has_more"]
            if len(lst_guest) == 0:
                return

            df = pd.DataFrame(lst_guest)
            # Apply the sanitization to each column
            for column in df.columns:
                df[column] = df[column].map(lambda x: re.sub(r"[\x00-\x1F]+", "", x) if isinstance(x, str) else x, na_action="ignore")

            guest_list_to_create = []
            for i in range(len(df)):
                try:
                    linkedin_url = df.loc[i, "user"].get(
                        "linkedin_handle", None
                    )
                    if (
                        pd.isnull(linkedin_url)
                        or "/company/" in linkedin_url
                        or "/school/" in linkedin_url
                    ):
                        continue

                    twitter_url = df.loc[i, "user"].get("twitter_handle", None)
                    website = df.loc[i, "user"].get("website", None)
                    name = df.loc[i, "user"].get("name", None)
                    time_zone = df.loc[i, "user"].get("timezone", None)

                    if pd.isnull(name) or name.strip() == "":
                        name = "Unavailable name"
                except Exception as e:
                    print("Error: ", e)
                    continue

                # Check if guest already exists for this event
                exists = self.session.query(GuestList).filter(
                    GuestList.linkedin_url == linkedin_url,
                    GuestList.event_id == event.id
                ).first()
                if exists:
                    count_existed += 1
                    continue

                # Check if guest exists in other events
                guest_exist = self.session.query(GuestList).filter(
                    GuestList.linkedin_url == linkedin_url
                ).first()
                
                if guest_exist:
                    print(f"--------> existed {linkedin_url}", guest_exist.name)
                    list_guest = GuestList(
                        name=guest_exist.name,
                        linkedin_url=guest_exist.linkedin_url,
                        twitter_url=guest_exist.twitter_url,
                        website=guest_exist.website,
                        event_id=event.id,
                        email=guest_exist.email or "",
                        role=guest_exist.role,
                        company_id=guest_exist.company_id,
                        check_company=False,
                        time_zone=time_zone
                    )
                    guest_list_to_create.append(list_guest)

                    # Create notification if company exists
                    if guest_exist.company_id:
                        self._get_or_create_notification(
                            reference_id=event.id,
                            type_="EVENT",
                            company=guest_exist.company,
                            defaults={
                                "title": event.name,
                                "post_url": "https://lu.ma/" + (event.event_url or ""),
                                "time_post": event.start_date,
                            },
                        )
                    count_adding += 1

                else:
                    try:
                        company = self.find_company_by_domain(website)
                        list_guest = GuestList(
                            name=name,
                            linkedin_url=linkedin_url,
                            twitter_url=twitter_url,
                            website=website,
                            event_id=event.id,
                            company_id=(company.id if company else None),
                            check_company=False if company is None else True,
                            time_zone=time_zone
                        )
                        guest_list_to_create.append(list_guest)
                        count_adding += 1
                        
                        self._get_or_create_notification(
                            reference_id=event.id,
                            type_="EVENT",
                            company=company,
                            defaults={
                                "title": event.name,
                                "post_url": "https://lu.ma/" + (event.event_url or ""),
                                "time_post": event.start_date,
                            },
                        )
                    except Exception as e:
                        print(f"Error when adding guest {linkedin_url}: {e}")
                        continue

            if guest_list_to_create:
                self.session.bulk_save_objects(guest_list_to_create)
                self.session.flush()
                print("##len guest_list_to_create: ", len(guest_list_to_create))

            if (not has_more) or ("next_cursor" not in rsp):
                if count_adding > 0:
                    print(f"          ---->  Adding: {count_adding} guests, existed: {count_existed} for event {event.name}")
                return

            next_pagination = rsp["next_cursor"]

    def getAllGuests(self):
        date_from = datetime.now().date() - timedelta(days=3)
        lst_event = (
            self.session.query(EventsList)
            .filter(EventsList.end_at >= date_from)
            .all()
        )
        print(f"        ------> Getting guests for {len(lst_event)} events")
        for event in lst_event:
            self.getGuestList(event)
            # self.updateEmailStatusForGuest(event.id)  # TODO: implement if needed

    def getEmailAllGuest(self):
        # Get guests with email = "waiting" or None or ""
        all_guests = (
            self.session.query(GuestList)
            .filter(
                or_(
                    GuestList.email == "waiting",
                    GuestList.email.is_(None),
                    GuestList.email == ""
                )
            )
            .all()
        )
        for guest in all_guests:
            # TODO: implement getEmailByLPersonUrl function
            # guest.email, guest.role = getEmailByLPersonUrl(guest.linkedin_url)
            guest.updated_at = datetime.now()
        self.session.flush()

    def findMissingCompany(self):
        from tqdm import tqdm
        
        # Get distinct linkedin_urls for guests without company
        all_guests = (
            self.session.query(GuestList.linkedin_url)
            .filter(
                GuestList.company_id.is_(None),
                GuestList.check_company == False
            )
            .distinct()
            .all()
        )
        print(f"        ------> Finding missing: {len(all_guests)} guests")
        success, fail = 0, 0
        with tqdm(total=len(all_guests), desc="Finding missing guests") as pbar:
            for guest_row in all_guests:
                linkedin_url = guest_row[0]
                company = self.findCompany(linkedin_url)
                if company:
                    self.session.query(GuestList).filter(
                        GuestList.linkedin_url == linkedin_url
                    ).update({
                        "company_id": company.id,
                        "check_company": True,
                        "updated_at": datetime.now()
                    })
                    success += 1
                else:
                    self.session.query(GuestList).filter(
                        GuestList.linkedin_url == linkedin_url
                    ).update({
                        "check_company": True,
                        "updated_at": datetime.now()
                    })
                    fail += 1
                pbar.set_postfix({"Success": success, "Fail": fail})
                pbar.update(1)
        self.session.flush()

    def updateNumberCompanyAndGuest(self):
        date_from = datetime.now().date() - timedelta(days=7)
        lst_event = (
            self.session.query(EventsList)
            .filter(EventsList.end_at >= date_from)
            .order_by(EventsList.start_date.desc())
            .all()
        )
        print(f"        ------> Getting guests for {len(lst_event)} events")
        for event in lst_event:
            event_id = event.id
            # Count guests for this event
            guest_count = self.session.query(GuestList).filter(
                GuestList.event_id == event_id
            ).count()
            
            # Count distinct companies for guests of this event
            company_count = (
                self.session.query(GuestList.company_id)
                .filter(
                    GuestList.event_id == event_id,
                    GuestList.company_id.isnot(None)
                )
                .distinct()
                .count()
            )
            
            event.number_of_company = company_count
            event.number_of_guest = guest_count
        self.session.flush()

    def updateEmailStatusForGuest(self, event_id):
        """
        TODO: Implement this method when MailHistory and MailAppAccount models are available.
        This method updates email status for guests based on mail history.
        """
        # Get guests for this event with company
        main_data = (
            self.session.query(GuestList)
            .filter(
                GuestList.event_id == event_id,
                GuestList.company_id.isnot(None)
            )
            .all()
        )
        
        # For now, just set all to UNREACHED since we don't have MailHistory model
        for data in main_data:
            if data.email == "Email unavailable":
                data.email_status_emailinfor = "EMAIL UNAVAILABLE"
                data.send_by_emailinfor = ""
                data.last_activity_emailinfor = None
                data.error_emailinfor = ""
                data.last_reply_emailinfor = None
            else:
                data.email_status_emailinfor = "UNREACHED"
                data.send_by_emailinfor = ""
                data.last_activity_emailinfor = None
                data.error_emailinfor = ""
                data.last_reply_emailinfor = None
        
        self.session.flush()
