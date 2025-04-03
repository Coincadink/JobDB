import os
from dotenv import load_dotenv
import requests
from datetime import datetime
from .base_scraper import Scraper

class ASMLScraper(Scraper):
    def __init__(self):
        super().__init__("ASML")
        self.api_url = "https://discover-euc1.sitecorecloud.io/discover/v2/126200477"

        load_dotenv("../.env")
        self.user_uuid = os.getenv("UUID_PREFIX") + str(int(datetime.now().timestamp() * 1000))
        self.auth_token = os.getenv("AUTH_TOKEN") # TODO: Revize if token breaks.

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:136.0) Gecko/20100101 Firefox/136.0",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.5",
            "Content-Type": "application/json",
            "authorization": self.auth_token,
            "Sec-GPC": "1",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
            "Priority": "u=0"
        }
    
    def scrape_jobs(self, filters=None):
        """Scrape all current job listings from ASML using their API"""
        self.logger.info("Starting ASML job scraping")
        
        jobs = []
        offset = 0
        limit = 100
        has_more = True
        
        while has_more:
            self.logger.info(f"Fetching ASML jobs with offset {offset}")
            
            payload = {
                "context": {
                    "page": {
                        "uri": "https://www.asml.com/en/careers/find-your-job?job_type=Fix&page=2"
                    },
                    "locale": {
                        "country": "us",
                        "language": "en"
                    },
                    "user": {
                        "uuid": "126200477-0q-y8-4p-1p-i4naathemk469znl6jls-1743631691881"
                    }
                },
                "widget": {
                    "items": [
                        {
                            "entity": "content",
                            "rfk_id": "asml_job_search",
                            "search": {
                                "limit": limit,
                                "offset": offset,
                                "content": {},
                                "filter": {
                                    "type": "and",
                                    "filters": [
                                        {"name": "job_type", "values": ["Fix"], "type": "anyOf"}
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
            
            try:
                response = requests.post(self.api_url, json=payload, headers=self.headers)
                
                # Print response status for debugging
                self.logger.info(f"Response status: {response.status_code}")
                response.raise_for_status()

                data = response.json()["widgets"][0]
                
                # Extract job listings from the response
                try:
                    total_jobs = data["total_item"]
                    
                    # Process each job
                    for job in data["content"]:
                        job_data = self.parse_job(job)
                        jobs.append(job_data)
                    
                    # Check if there are more jobs to fetch
                    offset += limit
                    if offset >= total_jobs:
                        has_more = False
                    
                except (KeyError, IndexError) as e:
                    self.logger.error(f"Error parsing API response: {e}")
                    self.logger.debug(f"Response content: {data}")
                    has_more = False
                    
            except requests.exceptions.HTTPError as e:
                self.logger.error(f"HTTP Error: {e}")
                has_more = False
                
                if hasattr(e, 'response') and e.response:
                    self.logger.debug(f"Error response content: {e.response.text}")
        
        self.logger.info(f"Found {len(jobs)} ASML jobs")
        return jobs
    
    def parse_job(self, job):
        """Parse job data from the API response"""
        return job["name"]