

def updateLinkedinUrl(linkedin_url):
    if linkedin_url is None:
        return None
    if "linkedin.com.in/" in linkedin_url:
        linkedin_url = linkedin_url.replace("linkedin.com.in/", "linkedin.com/in/")
    if "/" == linkedin_url[-1]:
        linkedin_url = linkedin_url[:-1]
    if "linkedin.com" not in linkedin_url:
        return "https://www.linkedin.com"
    if ("/in/" not in linkedin_url) and ("/company/" not in linkedin_url) and ("/school/" not in linkedin_url):
        return "https://www.linkedin.com"
    return f"https://www.linkedin.com/{linkedin_url.split('.com/')[1]}"
