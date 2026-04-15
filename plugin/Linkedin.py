import json
import logging
import re

import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import joinedload

from model.model import (
    LinkedinCategory,
    LinkedinCompany,
    LinkedinExcludeCompany,
    LinkedinExcludeKey,
    LinkedinJob,
    LinkedinJobLabels,
    LinkedinLocation,
    Notification,
)
import random

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import SSLError, ConnectionError, Timeout, RequestException
import time
from plugin.utils import IMPORTANT_KEYS, INDUSTRIES_REJECT
from plugin.utils.format_data import updateLinkedinUrl

DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": DEFAULT_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
# Nếu bạn bật lại GenAI thì uncomment + import đúng module
# from genAI.general_agent import (
#     GenCheckCategoryForCompany,
#     SummaryCompanyDescription,
#     SummaryJobDescription,
#     genLabelCompany,
#     genLabelJob,
# )

logger = logging.getLogger(__name__)


class Linkedin:
    def __init__(self, session):
        """
        session: SQLAlchemy Session (vd: airflow SQLAlchemyHook.get_session() trả về Session)
        """
        self.session = session
        self.lst_check = []

        # 1) Lấy all jobs + join company/category để tránh N+1 khi access job.company.name / job.category.name
        all_jobs = (
            self.session.query(LinkedinJob)
            .options(joinedload(LinkedinJob.company), joinedload(LinkedinJob.category))
            .all()
        )

        self.lst_check_file_old = [
            f"{job.title}{(job.company.name if job.company else '')}{(job.category.name if job.category else '')}"
            for job in all_jobs
        ]

        # 2) KEY_WORDS = list tên category
        self.KEY_WORDS = [
            x[0]
            for x in self.session.query(LinkedinCategory.name)
            .filter(LinkedinCategory.name.isnot(None))
            .all()
        ]

        # 3) LOCATIONS = list tên location
        self.LOCATIONS = [
            x[0]
            for x in self.session.query(LinkedinLocation.name)
            .filter(LinkedinLocation.name.isnot(None))
            .all()
        ]

        # 4) REJECT_KEY = list key
        self.REJECT_KEY = [
            x[0]
            for x in self.session.query(LinkedinExcludeKey.key)
            .filter(LinkedinExcludeKey.key.isnot(None))
            .all()
        ]

        # 5) COMPANY_REJECT_KEY = list company reject
        self.COMPANY_REJECT_KEY = [
            x[0]
            for x in self.session.query(LinkedinExcludeCompany.company)
            .filter(LinkedinExcludeCompany.company.isnot(None))
            .all()
        ]

        logger.info("Company reject key: %s", self.COMPANY_REJECT_KEY)

    # ---------------------------------------------------------------------
    # DB helpers (SQLAlchemy)
    # ---------------------------------------------------------------------
    def _get_company_by_linkedin_url(self, linkedin_url: str):
        return (
            self.session.query(LinkedinCompany)
            .filter(LinkedinCompany.linkedin_url == linkedin_url)
            .first()
        )

    def _get_category_by_name(self, name: str):
        return (
            self.session.query(LinkedinCategory)
            .filter(LinkedinCategory.name == name)
            .first()
        )

    def _get_location_by_name(self, name: str):
        return (
            self.session.query(LinkedinLocation)
            .filter(LinkedinLocation.name == name)
            .first()
        )

    def _get_job_label_by_name(self, name: str):
        return (
            self.session.query(LinkedinJobLabels)
            .filter(LinkedinJobLabels.name == name)
            .first()
        )

    def _get_or_create_notification(self, reference_id, type_, company, defaults: dict):
        """
        SQLAlchemy version of get_or_create
        """
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
        return n, True

    # ---------------------------------------------------------------------
    # Business logic
    # ---------------------------------------------------------------------
    def getCompany(self, linkCompany, company_name):
        linkedin_url = updateLinkedinUrl(linkCompany)

        company = self._get_company_by_linkedin_url(linkedin_url)
        if company:
            return True, company

        company_info = getCompanyInfor(linkCompany)
        if company_info.get("industry") in INDUSTRIES_REJECT:
            return False, None

        # Nếu dùng GenAI thì bật lại:
        # short_description = SummaryCompanyDescription().generate(company_info["description"], company_info["industry"])
        # label, category = genLabelCompany().generate(company_info["description"], company_info["industry"])
        # check_category = GenCheckCategoryForCompany().generate(description=company_info["description"])

        short_description = ""
        label = "Others"
        category = "Waiting"
        check_category = "Waiting"

        if category and (category != "Waiting"):
            company_info["category"] = category
            company_info["labels"] = label

        company = LinkedinCompany(
            name=company_name,
            linkedin_url=linkedin_url,
            website=company_info.get("website"),
            description=company_info.get("description"),
            industry=company_info.get("industry"),
            organization_type=company_info.get("organization_type"),
            headquarters=company_info.get("headquarters"),
            followers=company_info.get("followers"),
            size=company_info.get("size", "Waiting"),
            short_description=short_description,
            category=company_info.get("category"),
            labels=company_info.get("labels"),
            avatar_url=company_info.get("avatar_url"),
            is_crawl=0,
            is_finding_company=check_category if check_category == "Recruitment agency" else None,
        )

        self.session.add(company)
        self.session.flush()  # để có company.id ngay
        return True, company

    def getDescriptionByJobId(self, id_job):
        url = f"https://www.linkedin.com/jobs/view/{id_job}/"
        retry_count = 0
        res = ""

        while retry_count < 5:
            try:
                response = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=20)
                soup = BeautifulSoup(response.text, "html.parser")

                # LinkedIn thường trả page HTML đầy đủ
                if "<!DOCTYPE html>" in str(soup):
                    stronge = soup.find("section", {"class": "core-section-container my-3 description"})
                    if not stronge:
                        break

                    section = stronge.find("section", {"class": "show-more-less-html"})
                    if not section:
                        break

                    div_search = section.find(
                        "div",
                        {"class": "show-more-less-html__markup show-more-less-html__markup--clamp-after-5 relative overflow-hidden"},
                    )
                    if div_search:
                        res = div_search
                    break
                else:
                    retry_count += 1
            except Exception:
                retry_count += 1

        soup = BeautifulSoup(str(res), "html.parser")
        return soup.get_text(separator="\n")

    def getJobUsingRequests(self, request_url):
        try:
            max_attempts = 5
            li_elements = []

            session = requests.Session()

            retry_strategy = Retry(
                total=3,
                connect=3,
                read=3,
                backoff_factor=1.5,  # exponential backoff
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
                raise_on_status=False,
            )

            adapter = HTTPAdapter(
                max_retries=retry_strategy,
                pool_connections=5,
                pool_maxsize=5,
            )

            session.mount("https://", adapter)
            session.mount("http://", adapter)

            for attempt in range(1, max_attempts + 1):
                try:

                    resp = session.get(
                        request_url,
                        headers=DEFAULT_HEADERS,
                        timeout=(5, 20),  # (connect timeout, read timeout)
                    )
                    if resp.status_code != 200:
                        time.sleep(random.uniform(2, 5))
                        continue

                    if not resp.text or len(resp.text) < 500:
                        logger.warning(
                            "Empty or suspicious response body | URL: %s", request_url
                        )
                        time.sleep(random.uniform(2, 5))
                        continue

                    soup = BeautifulSoup(resp.text, "html.parser")

                    li_elements = soup.find_all("li")
                    logger.info("Number of <li> elements found: %d", len(li_elements))
                    if li_elements:
                        return li_elements

                    logger.warning(
                        "No <li> found, retrying... | URL: %s", request_url
                    )

                except (SSLError, ConnectionError, Timeout) as e:
                    logger.error(
                        "NetworkError %s: %s | URL: %s", type(e).__name__, e, request_url
                    )

                except RequestException as e:
                    logger.error(
                        "RequestException: %s | URL: %s", e, request_url
                    )

                # backoff + jitter để tránh bị block
                sleep_time = random.uniform(2, 6) * attempt
                time.sleep(sleep_time)

            return li_elements
        except Exception as e:
            import traceback
            traceback.print_exc()
            return []

    def getJobByKeyword(self, request_url, category, location, start, note: str = "Get data job daily"):
        li_elements = self.getJobUsingRequests(request_url)
        logger.info("Number of job elements retrieved: %d", len(li_elements))
        # cache object lookup để tránh query lặp
        category_obj = self._get_category_by_name(category)
        location_obj = self._get_location_by_name(location)

        for li in li_elements:
            job_id_div = li.find(
                "div",
                {"class": "base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card"},
            )
            a = li.find("a", {"class": "hidden-nested-link", "data-tracking-control-name": "public_jobs_jserp-result_job-search-card-subtitle"})
            job_tt = li.find("h3", {"class": "base-search-card__title"})

            if not (job_id_div and a and job_tt):
                continue

            job_id = job_id_div.get("data-entity-urn", "").split(":")[-1]
            linkCompany = str(a.get("href")).replace("?trk=public_jobs_jserp-result_job-search-card-subtitle", "")
            linkCompany = f"https://www.linkedin.com/company/{linkCompany.split('/')[-1]}"

            company_name = re.sub(r"[^\w\s]", "", a.text.strip())
            job_title = job_tt.text.strip()

            # reject rules
            if any(keyword.lower() in job_title.lower() for keyword in self.REJECT_KEY):
                continue
            if any(keyword.lower() in company_name.lower() for keyword in self.COMPANY_REJECT_KEY):
                continue

            # de-dup rules
            if f"{category}-{location}-{linkCompany}-{job_title}" in self.lst_check:
                continue
            if f"{job_title}{company_name}{category}" in self.lst_check_file_old:
                continue

            # important keywords filter
            if not any(keyword.lower() in job_title.lower() for keyword in IMPORTANT_KEYS):
                continue

            self.lst_check.append(f"{category}-{location}-{linkCompany}-{job_title}")

            is_get_company, company = self.getCompany(linkCompany, company_name)
            if not is_get_company:
                continue

            job_description = self.getDescriptionByJobId(job_id)

            # Nếu dùng GenAI thì bật lại:
            # label_name = genLabelJob().generate(category=category, job_description=job_description)
            # short_description = SummaryJobDescription().generate(description=job_description)
            label_name = "Others"
            short_description = ""

            label_obj = self._get_job_label_by_name(label_name)

            try:
                job = LinkedinJob(
                    title=job_title,
                    category_id=(category_obj.id if category_obj else None),
                    location_id=(location_obj.id if location_obj else None),
                    company_id=(company.id if company else None),
                    linkedin_url=f"https://www.linkedin.com/jobs/view/{job_id}",
                    description=job_description,
                    label_id=(label_obj.id if label_obj else None),
                    short_description=short_description,
                    note=note,
                )
                self.session.add(job)
                self.session.flush()  # để có job.id

                self._get_or_create_notification(
                    reference_id=job.id,
                    type_="HIRING",
                    company=company,
                    defaults={
                        "title": job.title,
                        "post_url": job.linkedin_url,
                        "time_post": getattr(job, "created_at", None),
                    },
                )

                # commit theo batch (tuỳ bạn). An toàn hơn: commit từng vòng keyword hoặc từng job.
                self.session.commit()

            except Exception as e:
                logger.exception("Error saving job: %s", e)
                self.session.rollback()
                continue

        start += len(li_elements)
        return start, len(li_elements)

    ############################################################################################################
    def getAllJob(self):
        for category in self.KEY_WORDS:
            for location in self.LOCATIONS:
                if location == "Others":
                    continue

                start = 0
                while True:
                    request_url = (
                        "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
                        f"?f_TPR=r86400&keywords={category}&location={location}"
                        "&origin=JOB_SEARCH_PAGE_SEARCH_BUTTON&refresh=true&position=1&pageNum=0"
                        f"&start={start}"
                    )
                    start, len_response = self.getJobByKeyword(request_url, category, location, start)
                    if len_response == 0:
                        break
                    if start >= 50:
                        break
                logger.info("Keyword: %s, Location: %s, Start: %d, Len: %d", category, location, start, len_response)

                if category == "Blockchain":
                    start = 0
                    while True:
                        request_url = (
                            "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
                            f"?f_TPR=r86400&keywords=web3&location={location}"
                            "&origin=JOB_SEARCH_PAGE_SEARCH_BUTTON&refresh=true&position=1&pageNum=0"
                            f"&start={start}"
                        )
                        start, len_response = self.getJobByKeyword(request_url, category, location, start)
                        if len_response == 0:
                            break
                        if start >= 50:
                            break
                    logger.info("Keyword: web3, Location: %s, Start: %d, Len: %d", location, start, len_response)


def getCompanyInfor(linkCompany) -> dict:
    rq = requests.get(linkCompany, headers={"User-Agent": DEFAULT_UA}, timeout=20)
    soup = BeautifulSoup(rq.text, "html.parser")
    company_info = {}
    if soup is None:
        return company_info

    # get company name
    title = soup.find("title").text if soup.find("title") else None
    company_info["name"] = title.split("|")[0].strip() if title else None

    # get website
    website = soup.find("div", {"data-test-id": "about-us__website"})
    company_info["website"] = (
        website.find("a").text.replace(" ", "").replace("\n", "").replace("\t", "").replace("\r", "").strip()
        if website and website.find("a")
        else None
    )

    # get industry
    industry = soup.find("div", {"data-test-id": "about-us__industry"})
    if industry and industry.find("dd"):
        industry_value = re.sub(r"\s{2,}", " ", industry.find("dd").text)
        company_info["industry"] = industry_value.replace("\n", "").replace("\t", "").replace("\r", "").strip()
    else:
        company_info["industry"] = None

    # get organization_type
    organization_type = soup.find("div", {"data-test-id": "about-us__organizationType"})
    if organization_type and organization_type.find("dd"):
        v = re.sub(r"\s{2,}", " ", organization_type.find("dd").text)
        company_info["organization_type"] = v.replace("\n", "").replace("\t", "").replace("\r", "").strip()
    else:
        company_info["organization_type"] = None

    # get headquarter
    headquarter = soup.find("div", {"data-test-id": "about-us__headquarters"})
    if headquarter and headquarter.find("dd"):
        headquarter_value = re.sub(r"\s{2,}", " ", headquarter.find("dd").text)
        cleaned_text = headquarter_value.replace("\n", "").replace("\t", "").replace("\r", "").strip()

        if cleaned_text.startswith("[{") and "fullAddress" in cleaned_text:
            try:
                parsed = json.loads(cleaned_text.replace("'", '"'))
                if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict) and "fullAddress" in parsed[0]:
                    company_info["headquarters"] = parsed[0]["fullAddress"].strip()
                else:
                    company_info["headquarters"] = cleaned_text
            except Exception:
                company_info["headquarters"] = cleaned_text
        else:
            company_info["headquarters"] = cleaned_text
    else:
        company_info["headquarters"] = None

    # fallback: org-locations
    location = soup.find("ul", {"data-impression-id": "org-locations_show-more-less"})
    if location:
        try:
            location = location.find("li").find("div").find_all("p")[-1].get_text(strip=True)
        except Exception:
            location = None
    else:
        location = None

    if company_info["headquarters"] is None:
        company_info["headquarters"] = location

    # get description
    description = soup.find("p", {"data-test-id": "about-us__description"})
    if description:
        v = re.sub(r"\s{2,}", " ", description.get_text(strip=True))
        company_info["description"] = v.replace("\n", "").replace("\t", "").replace("\r", "")
    else:
        company_info["description"] = None

    # get followers
    followers = soup.find("h3", class_="top-card-layout__first-subline")
    if followers:
        h3_value = followers.get_text().strip().lower()
        if "followers" in h3_value:
            value = h3_value.split("followers")[0].split()[-1]
        elif "follower" in h3_value:
            value = h3_value.split("follower")[0].split()[-1]
        else:
            value = None

        try:
            company_info["followers"] = int(value.replace(",", "")) if value else None
        except Exception as e:
            logger.warning("Parse follower failed: %s", e)
            company_info["followers"] = None
    else:
        meta = soup.find("meta", {"name": "twitter:description"})
        if meta and "content" in meta.attrs:
            content = meta.attrs["content"]
            m = re.search(r"(\d{1,3}(?:,\d{3})*|\d+)\s+follower[s]?", content)
            if m:
                try:
                    company_info["followers"] = int(m.group(1).replace(",", ""))
                except Exception as e:
                    logger.warning("Meta follower parse failed: %s", e)
                    company_info["followers"] = None
            else:
                company_info["followers"] = None
        else:
            company_info["followers"] = None

    # get size
    size = soup.find("div", {"data-test-id": "about-us__size"})
    if size and size.find("dd"):
        size_value = (
            size.find("dd").text.replace(" ", "").replace("\n", "").replace("\t", "").replace("\r", "").replace("employees", "")
        )
        size_value = (
            size_value.replace("1employee", "0-1")
            .replace("210", "2-10")
            .replace("1150", "11-50")
            .replace("51200", "51-200")
            .replace("201500", "201-500")
            .replace("5011000", "501-1000")
            .replace("10015000", "1001-5000")
            .replace("500110000", "5001-10000")
        )
        company_info["size"] = size_value
    else:
        company_info["size"] = None

    # get avatar
    try:
        script = soup.find("script", {"type": "application/ld+json"})
        logo = None
        if script and script.string:
            data = json.loads(script.string)
            if "@graph" in data:
                for item in data["@graph"]:
                    if item.get("@type") == "Organization" and "logo" in item:
                        logo = item["logo"].get("contentUrl")
                        break
            else:
                logo = data.get("logo", {}).get("contentUrl")
        company_info["avatar_url"] = logo
    except Exception:
        company_info["avatar_url"] = None

    return company_info
