import time
from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from model.model import (
    LinkedinCompany,
    Mentions,
    MentionsSubDomain,
    Notification,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class Subdomains:
    def __init__(self, session) -> None:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-cache")
        driver = webdriver.Chrome(options=chrome_options)
        self.driver = driver
        self.session = session

    def getSubdomains(self, namedomain: str) -> list[dict[str, str]]:
        result: list[dict[str, str]] = []
        driver = self.driver
        driver.get("https://subdomainfinder.io/")
        driver.find_element(
            By.XPATH, "/html/body/div/div/div[1]/div[2]/form/div[1]/input"
        ).send_keys(namedomain)

        wait = WebDriverWait(driver, 10)
        while True:
            choices = wait.until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, ".captcha-choice")
                )
            )
            print("Number of captcha choices found:", len(choices))
            for i in range(len(choices)):
                try:
                    print("---------->>> Trying captcha choice index:", i)
                    choices = driver.find_elements(By.CSS_SELECTOR, ".captcha-choice")
                    choice = choices[i]

                    # Dùng JS thay cho Selenium click()
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center'});", choice
                    )
                    driver.execute_script("arguments[0].click();", choice)

                    btn = driver.find_element(By.NAME, "scan")
                    btn.click()
                    time.sleep(2)
                    # wait.until(lambda d: 'Incorrect captcha' in d.page_source or 'success' in d.page_source)
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    if "Incorrect captcha" in soup.text:
                        print(
                            f"Captcha choice {i} was incorrect. Trying next choice..."
                        )
                        # driver.refresh()
                    else:
                        break

                except Exception as e:
                    import traceback

                    traceback.print_exc()
                    print(f"Error processing captcha choice {i}: {e}")
                    print(e)
                    continue
            else:
                continue

            break

        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "html.parser")

        table = soup.find("table", class_="table")
        if not table:
            print("------------->>>  No results table found on this domain :", namedomain)
            return result
        table_body = table.find("tbody")

        for tr in table_body.find_all("tr"):
            td_list = tr.find_all("td")
            href = td_list[0].find("a")["href"]
            date = td_list[1].text.strip()
            parsed_date = datetime.strptime(date, "%d %b %Y, %H:%M")
            result.append({"subdomain": href, "date": parsed_date})
        return result

    def getSubdomainsByLinkCompany(self, web_url: str) -> int:
        domain = ""
        if "http" not in web_url:
            domain = web_url
        if "www." in web_url:
            domain = web_url.split("www.")[-1]
            domain = domain.split("/")[0]
        else:
            domain = web_url.split("//")[-1]
        domain = domain.replace("/", "")
        domain = domain.lower()
        print("---------->>> DOMAIN :", domain)
        list_subdomain: list[dict[str, str]] = self.getSubdomains(domain)
        return self.saveToDB(web_url, list_subdomain)

    def saveToDB(self, web_url: str, subdomains: list[dict[str, str]]) -> int:
        company = (
            self.session.query(LinkedinCompany)
            .filter(LinkedinCompany.website == web_url)
            .first()
        )
        if not company:
            print(f"Company with web_url {web_url} not found in database.")
            return 0

        mentions_subdomain_old = [
            row[0]
            for row in (
                self.session.query(MentionsSubDomain.sub_domain)
                .join(Mentions, Mentions.id == MentionsSubDomain.mentions_id)
                .filter(Mentions.company_id == company.id)
                .all()
            )
        ]

        mentions_list = []
        subdomain_mentions_list = []
        notifications_list = []

        for subdomain_info in subdomains:
            subdomain_name = subdomain_info["subdomain"]
            if subdomain_name in mentions_subdomain_old:
                continue
            time_update = subdomain_info["date"]
            mentions = Mentions(
                company=company, type="SUB_DOMAIN", updated_at=time_update
            )
            mentions_list.append(mentions)
            subdomain_mention = MentionsSubDomain(
                sub_domain=subdomain_name,
                mentions=mentions,
                updated_at=time_update,
            )
            subdomain_mentions_list.append(subdomain_mention)

        # Bulk insert via SQLAlchemy
        self.session.add_all(mentions_list)
        self.session.add_all(subdomain_mentions_list)
        self.session.flush()
        self.session.commit()
        print("---------->>> Created Mentions:", len(mentions_list))
        print(
            "---------->>> Created Subdomain Mentions:", len(subdomain_mentions_list)
        )

        for subdomain_mention in subdomain_mentions_list:
            notification = Notification(
                reference_id=subdomain_mention.id,
                type="SUB_DOMAIN",
                company=company,
                title=subdomain_mention.sub_domain,
                time_post=subdomain_mention.updated_at,
            )
            notifications_list.append(notification)

        # Bulk create notifications
        self.session.add_all(notifications_list)
        self.session.flush()
        self.session.commit()
        print("---------->>> Created Notifications:", len(notifications_list))
        return len(subdomain_mentions_list)

    def driverQuit(self):
        self.driver.quit()
