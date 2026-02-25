import json

import os
import requests
from dotenv import load_dotenv
import random
import logging
load_dotenv()
LIST_APOLLO_KEY = os.getenv("API_KEY_APOLLO", None)
def getCompayData(response: requests.Response):
    try:
        company = response.json()
    except ValueError as e:
        logging.error(f"JSON Parsing Error: {e}")
        return None
    print('Company data retrieved:', company)
    if not company:
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
def getCompanyByUrl(link, type="domain") -> dict | None:
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
                company_data = getCompayData(response)
                if not company_data:
                    return None
                if company_data:
                    logging.info(f"Successfully retrieved company data for {link} using API key ending in {api_key[-4:]}")
                    return company_data
                else:
                    logging.warning(f"Company data for {link} is incomplete or missing 'organization' field.")
                logging.error(f"Attempt {attempt + 1}/{max_retries} failed with status {response.status_code}")
            except requests.RequestException as e:
                logging.error(f"Attempt {attempt + 1}/{max_retries} - Request Error: {e}")
            
            # Remove used key to try different one next time
            if len(api_keys) > 1:
                api_keys.remove(api_key)
                
    return company_data
