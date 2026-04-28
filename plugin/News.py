from __future__ import annotations
from model.model import NewsInformation, Notification, LinkedinCompany
import requests
import logging
import time
from plugin.utils.ai_helper import GetNameCompanyByPost
from bs4 import BeautifulSoup
from dateutil import parser
from datetime import datetime
logging.basicConfig(level=logging.INFO)


DEFAULT_NUMBER_OF_PAGES = 2
class NewsPlugin:
    def __init__(self, session) -> None:
        self.session = session
        self.gen_ai = GetNameCompanyByPost()
    
    
    
    def get_company_name_by_content(self, content: str):
        """Extract company name from content using AI"""
        if not content:
            return None
        
        try:
            result = self.gen_ai.generate(content)
            name_company = result.split(":")[1].strip() if ":" in result else result.strip()
            
            # Filter out common false positives
            if name_company.lower() in ["don't have data about name company", "have data about name", "techcrunch", "cryptonews", "dlnews"]:
                return None
            
            return name_company
        except Exception as e:
            logging.error(f"Error extracting company name: {e}")
            return None
    
    
    #---------------------- TechCrunch News ----------------------
    
    def get_detail_techcrunch_news(self, url: str) -> None:
        data = requests.get(url)
        soup = BeautifulSoup(data.text, 'html.parser')
        title = soup.find_all('div', class_='entry-content wp-block-post-content is-layout-constrained wp-block-post-content-is-layout-constrained')
        if not title:
            logging.warning(f"No content found for URL: {url}")
            return None
        title = title[0]
        return title.text.strip()
    
    def get_techcrunch_news(self,num_of_pages: int = DEFAULT_NUMBER_OF_PAGES) -> None:
        base_url = 'https://techcrunch.com/category/'
        categories = ['startups','artificial-intelligence']
        for category in categories:
            for page in range(1, num_of_pages + 1):
                url = f"{base_url}{category}/page/{page}/"
                data = requests.get(url)
                soup = BeautifulSoup(data.text, 'html.parser')
                ul_list = soup.select('ul.wp-block-post-template.is-layout-flow')
                
                news_list = []
                for ul in ul_list:
                    li_list = ul.find_all('li', recursive=False)
                    
                    for li in li_list:
                        try:
                            title = li.find('a', class_='loop-card__title-link').get_text()
                            author = li.find('a', class_='loop-card__author').get_text()
                            url = li.find('a', class_='loop-card__title-link').get('href')
                            time_elem = li.find('time', class_='loop-card__time')
                            datetime_value = time_elem.get('datetime') if time_elem else None
                            content = self.get_detail_techcrunch_news(url)
                            
                            logging.info(f"Title: {title} --- Author: {author} --- URL: {url}")
                            
                            # Check if news already exists
                            existing_news = self.session.query(NewsInformation).filter(
                                NewsInformation.link_news == url
                            ).first()
                            if existing_news:
                                logging.info(f"News already exists: {url}")
                                continue
                            
                            # Extract company name
                            company_name = self.get_company_name_by_content(content)
                            time.sleep(5)  # Sleep to avoid hitting AI service rate limits
                            logging.info(f"Company Name: {company_name}")
                            if not company_name:
                                logging.warning(f"Could not extract company name from: {url}")
                                continue
                            logging.info('----->>>')
                            # Get company object
                            company = self.session.query(LinkedinCompany).filter(
                                LinkedinCompany.name == company_name
                            ).first()
                            
                            # Parse datetime
                            if datetime_value:
                                try:
                                    time_post = parser.parse(datetime_value)
                                    formatted_time = time_post.strftime("%Y-%m-%d %H:%M:%S")
                                except:
                                    formatted_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            else:
                                formatted_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            # Create NewsInformation record
                            news = NewsInformation(
                                name_company=company_name,
                                company=company,
                                link_news=url,
                                content=content,
                                category="techcrunch-startups",
                                title=title,
                                time_post=formatted_time,
                            )
                            news_list.append(news)
                            
                            logging.info(f"Successfully prepared news for save: {title}")
                            time.sleep(2)
                            self.insert_news(news_list)
                            
                        except Exception as e:
                            logging.error(f"Error extracting data: {e}")
                            continue
                    
                        
    #---------------------- Insert News and Create Notification ----------------------           
    def insert_news(self, news_list: list[NewsInformation] | None) -> None:
        if not news_list:
            logging.info("No news to insert")
            return
        notification_list = []
        self.session.add_all(news_list)
        self.session.flush()
        self.session.commit()
        logging.info(f"Created {len(news_list)} news records")
        
        # Create notifications for each news
        for news in news_list:
            # Check if notification already exists
            existing_notification = self.session.query(Notification).filter(
                Notification.reference_id == news.id,
                Notification.type == "NEWS"
            ).first()
            
            if not existing_notification:
                notification = Notification(
                    reference_id=news.id,
                    type="NEWS",
                    company=news.company,
                    title=news.title,
                    post_url=news.link_news,
                    time_post=news.time_post,
                )
                notification_list.append(notification)
        
        # Bulk insert notifications
        if notification_list:
            self.session.add_all(notification_list)
            self.session.flush()
            self.session.commit()
            logging.info(f"Created {len(notification_list)} notification records")