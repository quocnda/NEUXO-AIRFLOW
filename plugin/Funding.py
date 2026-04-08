import http.client
import json
import time
from datetime import datetime

from bs4 import BeautifulSoup
from tqdm import tqdm

from model.model import CompanyFunding, LinkedinCompany, Notification
from plugin.Apollo import getCompanyByUrl
from plugin.Linkedin import getCompanyInfor
from plugin.utils import updateLinkedinUr