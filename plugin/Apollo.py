import json

import os
import requests


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
def getCompanyByUrl(link, type="linkedin_profile_url"):
    company_data = None
    if type == "domain":
        url = f"https://api.apollo.io/api/v1/organizations/enrich?domain={link}"
        headers = {
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "accept": "application/json",
            "x-api-key": "M4Yu3CY8sXPyanAbo2i6Zg",
        }
        try:
            response = requests.get(url, headers=headers)
        except requests.RequestException as e:
            print(f"Request Error: {e}")
            return None
        company_data = getCompayData(response)
    return company_data
