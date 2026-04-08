from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time


class SeleniumCrawler:
    
    def __init__(self):
        print('INIT WEB CRAWLER!!!!')
        self.driver = self.initialize_driver()
        
    def initialize_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=en-US")

        # User-Agent (không bắt buộc)
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    
    def get_link(self, url): 
        self.driver.get(url)
        time.sleep(2)  # Wait for page to load
    def extract_services_and_industries(self):

        try:

            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            services = []
            services_section = soup.find_all('li', class_ = 'chart-legend--item')
            for item in services_section:
                a = item.find("a", class_="chart-legend--item-link")
                if a:
                    services.append(a.get_text(strip=True)) 
            return services
        except Exception as e:
            print(f"Error occurred: {e}")
            self.driver.quit()
            self.driver = self.initialize_driver()
            return []
    def extract_services(self):
        return self.extract_services_and_industries()
    def _click_industries_tab_action(self):
        try:
            button = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="profile-chart-section"]/div/div[2]/h2[3]/button'))
            )

            button.click()
            time.sleep(5)
        except Exception as e:
            print(f"Error occurred while clicking industries tab: {e}")
            self.driver.quit()
            self.driver = self.initialize_driver()
    def extract_industries(self):
        self._click_industries_tab_action()
        return self.extract_services_and_industries()
    
    def get_industries_and_services(self, url):
        self.get_link(url)
        services = self.extract_services()
        industries = self.extract_industries()
        return {
            "services": services,
            "industries": industries
        }

    def fetch_services(self, url: str):
        """Fetch only services (no driver quit) for pooling reuse."""
        try:
            self.get_link(url)
            return self.extract_services()
        except Exception as e:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = self.initialize_driver()
            return []

    def close(self):
        try:
            self.driver.quit()
        except Exception:
            pass