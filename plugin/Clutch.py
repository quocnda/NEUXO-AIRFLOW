#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clutch.co crawler - Class-based, database-integrated version.

Features:
- curl_cffi Session + HTTP/2
- Rotate impersonate (chrome141 -> chrome120 -> safari17_0)
- Retry with exponential backoff + jitter on 403/429
- Upsert to LinkedinCompany table instead of CSV output
"""

from __future__ import annotations

import argparse
import logging
import random
import time
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **kwargs):
        return x

from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests as creq

from model.model import LinkedinCompany, ClutchReview
from plugin.utils.SeleniumCrawler import SeleniumCrawler
from plugin.utils.format_data import updateLinkedinUrl
logger = logging.getLogger(__name__)

# =========================
# Config
# =========================
DEFAULT_START_URL = "https://clutch.co/it-services"

COMMON_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://clutch.co/",
    "Upgrade-Insecure-Requests": "1",
}

IMP_CHOICES = ["chrome141", "chrome120", "safari17_0"]
SLEEP_MIN = 0.5
SLEEP_MAX = 1.5


class CrawlStatus(str, Enum):
    TIME_OUT = "TIME_OUT"
    ERROR = "ERROR"
    NO_REVIEW = "NO_REVIEW"
    HAVE_REVIEW = "HAVE_REVIEW"


class ClutchCrawler:
    """
    Crawler for Clutch.co company profiles.
    Updates/creates LinkedinCompany records in database.
    """

    def __init__(self, session: Session):
        """
        Args:
            session: SQLAlchemy Session from SQLAlchemyHook.get_session()
        """
        self.session = session
        # Store engine for creating thread-local sessions
        self._engine = session.get_bind()
        self._SessionFactory = __import__('sqlalchemy.orm', fromlist=['sessionmaker']).sessionmaker(
            bind=self._engine, autoflush=False, autocommit=False
        )
        self.http_session = creq.Session(headers=COMMON_HEADERS, timeout=30)
        self._company_cache: Dict[str, Dict[str, Any]] = {}
        import threading
        self._local = threading.local()
        self.web_crawler = SeleniumCrawler()

    # =========================
    # HTTP Helper
    # =========================
    def _http_get(self, url: str, max_retries: int = 4) -> Optional[creq.Response]:
        """GET with retry/backoff. Switch impersonate on 403/429."""
        delay = 2.0
        for i in range(max_retries):
            imp = IMP_CHOICES[min(i, len(IMP_CHOICES) - 1)]
            try:
                r = self.http_session.get(url, impersonate=imp, allow_redirects=True)
                if r.status_code == 200:
                    return r
                if r.status_code in (403, 429):
                    time.sleep(delay + random.uniform(0.5, 1.8))
                    delay *= 1.8
                    continue
                time.sleep(1.0)
            except Exception:
                time.sleep(delay)
                delay *= 1.8
        return None

    # =========================
    # Parsers
    # =========================
    @staticmethod
    def _extract_review_data(soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract project review data from a review element."""
        container = soup.find("div", class_="profile-review__data")
        if not container:
            return {}

        result: Dict[str, Any] = {}
        li_items = container.select("ul.data--list > li.data--item")

        for li in li_items:
            tooltip_html = li.get("data-tooltip-content", "")
            tooltip_label = (
                BeautifulSoup(tooltip_html, "html.parser").get_text(strip=True)
                if tooltip_html
                else None
            )
            text_parts = [
                t.strip()
                for t in li.stripped_strings
                if not t.lower().startswith("show more")
            ]
            if not text_parts:
                continue

            if tooltip_label:
                result[tooltip_label] = " ".join(text_parts)
            else:
                result.setdefault("unknown", []).append(" ".join(text_parts))
        return result

    @staticmethod
    def _extract_reviewer_info(soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract reviewer information from a review element."""
        container = soup.find("div", class_="profile-review__reviewer")
        if not container:
            return {}

        result: Dict[str, Any] = {}

        name_tag = container.find("div", class_="reviewer_card--name")
        if name_tag:
            result["reviewer_name"] = name_tag.get_text(strip=True)

        position_tag = container.find("div", class_="reviewer_position")
        if position_tag:
            text = position_tag.get_text(strip=True)
            result["reviewer_position_raw"] = text
            if "," in text:
                parts = [p.strip() for p in text.split(",", 1)]
                result["reviewer_role"] = parts[0]
                result["reviewer_company"] = parts[1]
            else:
                result["reviewer_role"] = text

        verified_tag = container.find(
            "span", class_="profile-review__reviewer-verification-badge-title"
        )
        if verified_tag:
            result["verified_status"] = verified_tag.get_text(strip=True)

        list_items = container.select("ul.reviewer_list > li.reviewer_list--item")
        for li in list_items:
            tooltip_html = li.get("data-tooltip-content", "")
            label = (
                BeautifulSoup(tooltip_html, "html.parser").get_text(strip=True)
                if tooltip_html
                else None
            )
            value_tag = li.find("span", class_="reviewer_list__details-title")
            value = value_tag.get_text(strip=True) if value_tag else None
            if label and value:
                result[label] = value

        return result

    @staticmethod
    def _extract_social_links(scope: BeautifulSoup) -> Dict[str, str]:
        """Extract social links from the contact section."""
        links: Dict[str, str] = {}
        social_links = scope.select(
            "div.profile-social-media__wrap a.profile-social-media__link"
        )
        for a in social_links:
            label = a.get("data-type") or a.get_text(strip=True)
            href = a.get("href")
            if label and href:
                links[label.lower()] = href
        return links

    def _extract_services(self, company_url: str) -> List[str]:
        """Extract services using Selenium fallback."""
        try:
            crawler = self.web_crawler
            data = crawler.get_industries_and_services(company_url)
            return data.get("services", []) or []
        except Exception:
            self.web_crawler.close()
            self.web_crawler = SeleniumCrawler()
            return []

    # =========================
    # DB Operations
    # =========================
    def _get_company_by_linkedin_company(self, linkedin_url: str) -> Optional[LinkedinCompany]:
        """Find existing company by LinkedIn URL."""
        if not linkedin_url:
            return None
        linkedin_url = updateLinkedinUrl(linkedin_url)
        sess = self.session
        return (
            sess.query(LinkedinCompany)
            .filter(LinkedinCompany.linkedin_url == linkedin_url)
            .first()
        )

    def _get_company_by_name(self, name: str) -> Optional[LinkedinCompany]:
        """Find existing company by name."""
        if not name:
            return None
        sess = self.session
        return (
            sess.query(LinkedinCompany)
            .filter(LinkedinCompany.name == name)
            .first()
        )

    def _upsert_company(self, company_data: Dict[str, Any]) -> Tuple[LinkedinCompany, bool]:
        """
        Update or create a LinkedinCompany record.
        
        Args:
            company_data: Dict with keys: name, website, description, services, 
                          linkedin_url, twitter_url, etc.
            session: Optional session (uses thread-local if not provided)
        
        Returns:
            Tuple of (company, created) where created is True if new record.
        """
        sess = self.session
        website = company_data.get("website")
        name = company_data.get("name")
        
        # Try to find existing company
        company = self._get_company_by_linkedin_company(company_data.get("linkedin_url", None))
        if not company and name:
            company = self._get_company_by_name(name)
        
        created = False
        if company:
            # Update existing company
            if company_data.get("description") and not company.description:
                company.description = company_data.get("description")
            if company_data.get("website") and not company.website:
                company.website = company_data.get("website")
            if company_data.get("linkedin_url") and not company.linkedin_url:
                company.linkedin_url = company_data.get("linkedin_url")
            if company_data.get("twitter_url") and not company.link_twitter:
                company.link_twitter = company_data.get("twitter_url")
            if company_data.get("services"):
                # Store services in labels as JSON
                existing_labels = company.labels or []
                services = company_data.get("services", [])
                if isinstance(services, str):
                    services = [s.strip() for s in services.split(",") if s.strip()]
                merged = list(set(existing_labels + services))
                company.labels = merged
            if company_data.get("size") and not company.size:
                company.size = company_data.get("size")
            if company_data.get("industry") and not company.industry:
                company.industry = company_data.get("industry")
            if company_data.get("note"):
                existing_note = company.note or ""
                new_note = company_data.get("note", "")
                if new_note and new_note not in existing_note:
                    company.note = f"{existing_note}\n{new_note}".strip()
        else:
            # Create new company
            company = LinkedinCompany(
                name=name or "Unknown",
                website=website,
                description=company_data.get("description"),
                linkedin_url=company_data.get("linkedin_url"),
                link_twitter=company_data.get("twitter_url"),
                size=company_data.get("size"),
                industry=company_data.get("industry"),
                labels=company_data.get("services") if isinstance(company_data.get("services"), list) else [],
                note=company_data.get("note"),
                is_finding_company="clutch",
            )
            sess.add(company)
            created = True
        
        return company, created

    def _get_thread_session(self) -> Session:
        """Get or create a thread-local session."""
        if not hasattr(self._local, 'session') or self._local.session is None:
            self._local.session = self._SessionFactory()
        return self._local.session

    def _close_thread_session(self):
        """Close the thread-local session if it exists."""
        if hasattr(self._local, 'session') and self._local.session is not None:
            try:
                self._local.session.close()
            except Exception:
                pass
            self._local.session = None

    def _commit_with_retry(self, max_retries: int = 3, session: Session = None) -> bool:
        """Commit session with retry on failure."""
        sess = session or self._get_thread_session()
        for attempt in range(max_retries):
            try:
                sess.commit()
                return True
            except Exception as e:
                logger.error(f"Commit failed (attempt {attempt + 1}): {e}")
                sess.rollback()
                if attempt < max_retries - 1:
                    time.sleep(1)
        return False

    def _get_company_by_linkedin_url(self, linkedin_url: str, session: Session = None) -> Optional[LinkedinCompany]:
        """Find existing company by LinkedIn URL."""
        if not linkedin_url:
            return None
        sess = session or self._get_thread_session()
        return (
            sess.query(LinkedinCompany)
            .filter(LinkedinCompany.linkedin_url == linkedin_url)
            .first()
        )

    def s_upsert_review(
        self,
        review_data: Dict[str, Any],
        company: LinkedinCompany,
    ) -> Tuple[ClutchReview, bool]:
        """
        Create a ClutchReview record linked to LinkedinCompany via linkedin_url.
        
        Args:
            review_data: Dict with review info (reviewer_name, reviewer_company, etc.)
            company: LinkedinCompany instance
            session: Optional session (uses thread-local if not provided)
        
        Returns:
            Tuple of (review, created) where created is True if new record.
        """
        sess = self.session
        # Find linked company by linkedin_url
        company_id = company.id if company else None
        
        # Check if review already exists (by reviewer_name + reviewer_company + company_id)
        existing_review = None
        reviewer_name = review_data.get("reviewer_name")
        reviewer_company = review_data.get("reviewer_company")
        
        if reviewer_name and reviewer_company:
            query = sess.query(ClutchReview).filter(
                ClutchReview.reviewer_name == reviewer_name,
                ClutchReview.reviewer_company == reviewer_company
            )
            if company_id:
                query = query.filter(ClutchReview.company_id == company_id)
            existing_review = query.first()
        
        if existing_review:
            # Update existing review if needed
            return existing_review, False
        
        # Create new review
        review = ClutchReview(
            company_id=company_id,
            reviewer_name=reviewer_name,
            reviewer_role=review_data.get("reviewer_role"),
            reviewer_company=reviewer_company,
            industry=review_data.get("Industry"),
            location=review_data.get("Location"),
            client_size=review_data.get("Client size"),
            services=review_data.get("Services"),
            project_size=review_data.get("Project size"),
            project_length=review_data.get("Project length"),
            project_description=review_data.get("Project description"),
            background=review_data.get("background"),
            website_url=review_data.get("website_url"),
            linkedin_url=company.linkedin_url if company else None,
            description_company_outsource=review_data.get("description_company_outsource"),
            services_company_outsource=review_data.get("services_company_outsource"),
        )
        sess.add(review)
        return review, True

    # =========================
    # Crawlers
    # =========================
    def get_company_urls(self, start_url: str) -> List[str]:
        """Get list of company profile URLs from directory page."""
        urls: List[str] = []
        resp = self._http_get(start_url)
        if not resp:
            logger.error("Cannot fetch directory page (403/timeout)")
            return urls

        soup = BeautifulSoup(resp.text, "html.parser")
        providers_list = soup.find(id="providers__list")
        if not providers_list:
            logger.error("Cannot find providers__list (possibly blocked)")
            return urls

        for li in providers_list.find_all("li", class_="provider-list-item"):
            cta = li.find("div", class_="provider__cta-container")
            if not cta:
                continue
            a = cta.find(
                "a",
                class_="provider__cta-link sg-button-v2 sg-button-v2--secondary directory_profile",
            )
            if a and a.get("href"):
                urls.append(urljoin("https://clutch.co", a["href"]))
        return urls

    def crawl_company_detail(self, company_url: str) -> Tuple[CrawlStatus, Dict[str, Any]]:
        """
        Crawl a single company page and extract data.
        
        Returns:
            Tuple of (status, company_data)
            status: "HAVE_REVIEW", "NO_REVIEW", "TIME_OUT", "ERROR"
        """
        resp = self._http_get(company_url)
        if not resp:
            logger.warning(f"Skipped (403/timeout): {company_url}")
            return CrawlStatus.TIME_OUT, {}

        soup = BeautifulSoup(resp.text, "html.parser")
        prefix_url = company_url.split("?page=")[0]

        # Get company info (cached)
        if prefix_url not in self._company_cache:
            description_el = soup.find(
                "section", class_="profile-summary profile-summary__section profile-section"
            )
            services = self._extract_services(company_url)
            
            website_container = soup.find("div", class_="profile-header__short-actions")
            website_url = None
            if website_container:
                li_website = website_container.find(
                    "li", class_="profile-short-actions__item profile-short-actions__item--visit-website"
                )
                if li_website:
                    a_website = li_website.find("a")
                    if a_website and a_website.get("href"):
                        website_url = str(a_website["href"])

            # Get company name
            name_el = soup.find("h1", class_="profile-header__title")
            company_name = name_el.get_text(strip=True) if name_el else None

            self._company_cache[prefix_url] = {
                "name": company_name,
                "description": description_el.get_text(strip=True) if description_el else None,
                "services": services,
                "website": website_url,
            }

        company_info = self._company_cache[prefix_url]

        # Get social links
        contact_scope = soup.find("section", id="contact") or soup
        social_links = self._extract_social_links(contact_scope)

        # Get reviews for additional client company data
        reviews_wrap = soup.find("div", class_="profile-reviews--list__wrapper")
        client_companies: List[Dict[str, Any]] = []
        reviews: List[Dict[str, Any]] = []
        
        if reviews_wrap:
            elements = reviews_wrap.find_all("article", class_="profile-review")
            logger.info(f"{company_url} -> {len(elements)} reviews found")

            for e in elements:
                project_data = self._extract_review_data(e)
                reviewer_data = self._extract_reviewer_info(e)
                
                # Extract description and background text
                desc_el = e.find("div", class_="profile-review__summary mobile_hide")
                viewmore_data = e.find("div", class_="profile-review__extra mobile_hide desktop-review-to-hide hidden")
                background_text = ""
                if viewmore_data:
                    background_el = viewmore_data.find("div", class_="profile-review__text with-border profile-review__extra-section")
                    background_text = background_el.get_text(strip=True) if background_el else ""
                
                # Build review data for ClutchReview table
                review_record = {
                    "reviewer_name": reviewer_data.get("reviewer_name"),
                    "reviewer_role": reviewer_data.get("reviewer_role"),
                    "reviewer_company": reviewer_data.get("reviewer_company"),
                    "Industry": reviewer_data.get("Industry"),
                    "Location": reviewer_data.get("Location"),
                    "Client size": reviewer_data.get("Client size"),
                    "Services": project_data.get("Services"),
                    "Project size": project_data.get("Project size"),
                    "Project length": project_data.get("Project length"),
                    "Project description": desc_el.get_text(strip=True) if desc_el else None,
                    "background": background_text,
                    "website_url": company_info.get("website"),
                    "description_company_outsource": company_info.get("description"),
                    "services_company_outsource": ", ".join(company_info.get("services", [])) if company_info.get("services") else None,
                }
                reviews.append(review_record)
                
                # Extract client company info from reviews
                client_name = reviewer_data.get("reviewer_company")
                if client_name:
                    client_data = {
                        "name": client_name,
                        "industry": reviewer_data.get("Industry"),
                        "size": reviewer_data.get("Client size"),
                        "note": f"From Clutch review - Services used: {project_data.get('Services', '')}",
                    }
                    client_companies.append(client_data)

        # Build outsource company data
        outsource_company_data = {
            "name": company_info.get("name"),
            "website": company_info.get("website"),
            "description": company_info.get("description"),
            "services": company_info.get("services"),
            "linkedin_url": updateLinkedinUrl(social_links.get("linkedin")),
            "twitter_url": social_links.get("twitter"),
            "note": "Outsource company from Clutch.co",
        }

        result = {
            "outsource_company": outsource_company_data,
            "client_companies": client_companies,
            "reviews": reviews,
        }

        status = CrawlStatus.HAVE_REVIEW if reviews else CrawlStatus.NO_REVIEW
        return status, result

    def process_company(self, company_url: str) -> Tuple[int, int, int, str]:
        created_count = 0
        updated_count = 0
        reviews_created = 0
        
        # Get thread-local session
        session = self.session

        try:
            for page in range(5):
                url_review = company_url if page == 0 else f"{company_url}?page={page}#reviews"

                status, data = self._crawl_with_retry(url_review)
                if status in (CrawlStatus.TIME_OUT, CrawlStatus.ERROR):
                    break
                if status == CrawlStatus.NO_REVIEW and page > 0:
                    break

                # Save outsource company
                if data.get("outsource_company") and page == 0:
                    company, created = self._upsert_company(data["outsource_company"])
                    if created:
                        created_count += 1
                    else:
                        updated_count += 1

                # Save reviews to ClutchReview table
                for review_data in data.get("reviews", []):
                    _, created = self._upsert_review(review_data, company)
                    if created:
                        reviews_created += 1

                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            
            # Commit at the end of processing this company
            self._commit_with_retry(session=self.session)
        except Exception as e:
            logger.error(f"Error processing {company_url}: {e}")
            self.session.rollback()
        finally:
            self._close_thread_session()

        return created_count, updated_count, reviews_created, company_url

    def _crawl_with_retry(
        self, url: str, max_retry: int = 3, backoff_base: float = 0.8
    ) -> Tuple[CrawlStatus, Dict[str, Any]]:
        """Crawl with retry logic."""
        for attempt in range(1, max_retry + 1):
            try:
                return self.crawl_company_detail(url)
            except Exception as e:
                logger.error(f"Crawl error (attempt {attempt}): {e}")
                if attempt == max_retry:
                    return CrawlStatus.ERROR, {}
                time.sleep((backoff_base ** attempt) + random.uniform(0.05, 0.25))
        return CrawlStatus.TIME_OUT, {}

    def run(
        self,
        start_url: str = DEFAULT_START_URL,
        max_pages: int = 2,
        start_page: int = 1,
        end_page: Optional[int] = None,
    ) -> Dict[str, int]:
        """
        Main crawl runner.
        
        Args:
            start_url: Base directory URL
            max_pages: Maximum pages to crawl (used if end_page is not set)
            start_page: First page number to crawl (1-based)
            end_page: Last page number to crawl (inclusive)
            workers: Number of concurrent workers
            flush_every: Commit to DB every N companies
        
        Returns:
            Dict with stats: {"created": N, "updated": M, "reviews_created": R, "total_companies": K}
        """
        total_created = 0
        total_updated = 0
        total_reviews_created = 0
        processed_urls = set()

        if end_page is None:
            end_page = start_page + max_pages - 1

        for page_num in range(start_page, end_page + 1):
            page_url = start_url if page_num == 1 else f"{start_url}?page={page_num}"

            company_urls = self.get_company_urls(page_url)
            if not company_urls:
                logger.warning(f"No companies found on page {page_num}. Stopping.")
                break
            print(f"---------->>>>  Page {page_num}: {len(company_urls)} company URLs found")
            batch_created = 0
            batch_updated = 0
            batch_reviews = 0

            pbar = tqdm(company_urls, total=len(company_urls), desc=f"Page {page_num}")
            for company_url in pbar:
                created, updated, reviews_created, url = self.process_company(company_url)
                batch_created += created
                batch_updated += updated
                batch_reviews += reviews_created
                processed_urls.add(url)

            total_created += batch_created
            total_updated += batch_updated
            total_reviews_created += batch_reviews
            logger.info(
                f"Page {page_num} done: {batch_created} created, "
                f"{batch_updated} updated, {batch_reviews} reviews"
            )

        return {
            "created": total_created,
            "updated": total_updated,
            "reviews_created": total_reviews_created,
            "total_companies": len(processed_urls),
        }
