from __future__ import annotations
from model.model import CompanyFunding, LinkedinCompany, Notification

from bs4 import BeautifulSoup
import http.client
from typing import Dict, Optional
import cloudscraper
from datetime import datetime
from plugin.utils.format_data import updateLinkedinUrl
import json

BASE_URL = "https://www.coincarp.com"
class CoinCarpScraper:
    def __init__(self, session) -> None:
        self.scraper = cloudscraper.create_scraper()
        self.session = session
        pass
    
    
    def get_data_funding(self, start=0) -> list[dict[str , str | int]] | None:
        conn = http.client.HTTPSConnection("sapi.coincarp.com")
        payload = ''
        headers = {}
        conn.request("GET", f"/api/v1/market/fundraising/list?lang=en-US&draw=1&start={start}&length=20", payload, headers)
        res = conn.getresponse()
        data = res.read()
        data = json.loads(data.decode("utf-8"))
        if data['data']['list']:
            return data['data']['list']
        return None
    
    def get_social_links(self,project_id: str) -> Optional[Dict[str, str]]:
        try:
            scraper = self.scraper
            
            url = f"{BASE_URL}/project/{project_id}/"
            response = scraper.get(url, timeout=100)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            social_list = soup.find("div", class_="social-list d-flex")
            social_links = {}
            
            if social_list:
                links = social_list.find_all("a")
                for link in links:
                    title = link.get("data-original-title", "Unknown")
                    href = link.get("href", "")
                    if title and href:
                        social_links[title] = href                
                return social_links
            else:
                print("⚠️ Không tìm thấy social links")
                return None
                
        except Exception as e:
            print(f"❌ Lỗi: {str(e)}")
            return None
    def insert_data(self, page: int = 1) -> None:
        list_data_funding = self.get_data_funding(start=page)
        if not list_data_funding:
            return
        print(f"Total projects fetched: {len(list_data_funding)}")
        new_funding_list = []
        has_updates = False

        for project in list_data_funding:
            project_id = project.get("projectcode")
            social_links = self.get_social_links(project_id) if project_id else None
            website = social_links.get("Website", "") if social_links else ""
            linkedin_url = social_links.get("Linkin", "") if social_links else ""
            linkedin_url = updateLinkedinUrl(linkedin_url) if linkedin_url else None

            company = None
            if linkedin_url:
                company = (
                    self.session.query(LinkedinCompany)
                    .filter(LinkedinCompany.linkedin_url == linkedin_url)
                    .first()
                )

            name = project.get("projectname")
            round = project.get("fundstagename")
            date = datetime.fromtimestamp(project["funddate"]).strftime("%Y-%m-%d %H:%M:%S")
            amount = project.get("fundamount")
            category = ",".join([cat["name"] for cat in project.get("categorylist", [])])
            logo_url = "https://s1.coincarp.com/" + project.get("logo", "")

            fund_code = project.get("fundcode") or project_id
            project_url = None
            if fund_code:
                if project.get("fundcode"):
                    project_url = f"{BASE_URL}/fundraising/{fund_code}"
                else:
                    project_url = f"{BASE_URL}/project/{fund_code}/"

            defaults = {
                "name": name,
                "round": round,
                "date": date,
                "amount": amount,
                "category": category,
                "website": website,
                "linkedin_url": linkedin_url,
                "logo_url": logo_url,
                "company_id": company.id if company else None,
                "project_url": project_url,
            }

            existing_funding = None
            if project_url:
                existing_funding = (
                    self.session.query(CompanyFunding)
                    .filter(CompanyFunding.project_url == project_url)
                    .first()
                )

            if not existing_funding and name and date:
                existing_funding = (
                    self.session.query(CompanyFunding)
                    .filter(CompanyFunding.name == name, CompanyFunding.date == date)
                    .first()
                )

            if existing_funding:
                for key, value in defaults.items():
                    setattr(existing_funding, key, value)
                has_updates = True
                continue

            new_funding_list.append(CompanyFunding(**defaults))

        if new_funding_list:
            self.session.add_all(new_funding_list)
            self.session.flush()

        if new_funding_list or has_updates:
            self.session.commit()

        if not new_funding_list:
            return

        notification_list = []
        for funding in new_funding_list:
            if funding.linkedin_url and not funding.company:
                funding.company = (
                    self.session.query(LinkedinCompany)
                    .filter(LinkedinCompany.linkedin_url == funding.linkedin_url)
                    .first()
                )

            if not funding.company_id:
                continue

            existing_notification = (
                self.session.query(Notification)
                .filter(
                    Notification.reference_id == funding.id,
                    Notification.type == "FUNDING",
                )
                .first()
            )

            if existing_notification:
                continue

            if not funding.amount or str(funding.amount) == "0":
                title = "Company has received new funding"
            else:
                title = f"Company has received ${funding.amount} funding"

            notification_list.append(
                Notification(
                    reference_id=funding.id,
                    type="FUNDING",
                    company=funding.company,
                    title=title,
                    post_url=funding.project_url,
                    time_post=funding.date,
                )
            )

        if notification_list:
            self.session.add_all(notification_list)
            self.session.flush()
            self.session.commit()