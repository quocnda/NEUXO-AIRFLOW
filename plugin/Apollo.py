import json

import os
import requests
from dotenv import load_dotenv
import random

load_dotenv()
LIST_APOLLO_KEY = os.getenv("API_KEY_APOLLO", None)
def getCompayData(response: requests.Response):
    try:
        company = response.json()
    except ValueError as e:
        print(f"JSON Parsing Error: {e}")
        return None
    if company is None or "organization" not in company:
        return None

    if "linkedin_url" not in company["organization"]:
        return None
    company_info = {
            "company_linkedin_url": (
                company["organization"]["linkedin_url"]
                if "linkedin_url" in company["organization"]
                else None
            ),
            "originzation_id": (
                company["organization"]["id"]
                if "id" in company["organization"]
                else None
            ),
            "website": (
                company["organization"]["website_url"]
                if "website_url" in company["organization"]
                else None
            ),
            "short_description": (
                company["organization"]["short_description"]
                if "short_description" in company["organization"]
                else None
            ),
            "total_funding": (
                company["organization"]["total_funding"]
                if "total_funding" in company["organization"]
                else None
            ),
            "latest_funding_round_date": (
                company["organization"]["latest_funding_round_date"]
                if "latest_funding_round_date" in company["organization"]
                else None
            ),
            "size": (
                company["organization"]["estimated_num_employees"]
                if "estimated_num_employees" in company["organization"]
                else None
            ),
            "name": company["organization"]["name"],
            "linkedin_uid": (
                company["organization"]["linkedin_uid"]
                if "linkedin_uid" in company["organization"]
                else None
            ),
        }
    return company_info
def getCompanyByUrl(link, type="domain"):
    company_data = None
    api_keys = LIST_APOLLO_KEY.split(",")
    max_retries = min(3, len(api_keys))
    
    if type == "domain":
        url = f"https://api.apollo.io/api/v1/organizations/enrich?domain={link}"
        
        for attempt in range(max_retries):
            api_key = random.choice(api_keys)
            headers = {
                "Cache-Control": "no-cache",
                "Content-Type": "application/json",
                "accept": "application/json",
                "x-api-key": api_key,
            }
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    company_data = getCompayData(response)
                    if company_data:
                        return company_data
                print(f"Attempt {attempt + 1}/{max_retries} failed with status {response.status_code}")
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1}/{max_retries} - Request Error: {e}")
            
            # Remove used key to try different one next time
            if len(api_keys) > 1:
                api_keys.remove(api_key)
                
    return company_data
