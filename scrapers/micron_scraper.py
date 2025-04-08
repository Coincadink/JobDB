import os
from dotenv import load_dotenv
import requests
from datetime import datetime
from .base_scraper import Scraper

class MicronScraper(Scraper):
    def __init__(self):
        super().__init__("Micron")
        self.api_url = "https://careers.micron.com/api/apply/v2/jobs"

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:136.0) Gecko/20100101 Firefox/136.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Referer": "https://careers.micron.com/careers",
            "content-type": "application/json",
            "DNT": "1",
            "Sec-GPC": "1",
            "Alt-Used": "careers.micron.com",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Priority": "u=0",
            "Cache-Control": "max-age=0"
        }
    
    def scrape_jobs(self, filters=None):
        """Scrape all current job listings from Micron using their API"""
        self.logger.info("Starting Micron job scraping")
        
        jobs = []
        offset = 0
        limit = 10
        has_more = True
        
        while has_more:
            self.logger.info(f"Fetching Micron jobs with offset {offset}")
            # Build URL with query parameters. The 'domain' and 'sort_by' parameters are fixed.
            current_url = (
                f"{self.api_url}?domain=micron.com&start={offset}&num={limit}&sort_by=relevance"
            )
            try:
                response = requests.get(current_url, headers=self.headers)
                self.logger.info(f"Response status: {response.status_code}")
                response.raise_for_status()
                
                data = response.json()
                # The API returns the total job count and a list of positions
                total_jobs = data.get("count", 0)
                positions = data.get("positions", [])
                
                for job in positions:
                    job_data = self.parse_job(job)
                    jobs.append(job_data)
                
                offset += limit
                if offset >= total_jobs:
                    has_more = False
                    
            except requests.exceptions.HTTPError as e:
                self.logger.error(f"HTTP Error: {e}")
                if hasattr(e, 'response') and e.response:
                    self.logger.debug(f"Error response content: {e.response.text}")
                has_more = False
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                has_more = False
        
        self.logger.info(f"Found {len(jobs)} Micron jobs")
        return jobs
    
    def parse_job(self, job):
        """Parse job data from Micron's API response"""
        try:
            # Convert the creation timestamp (t_create) to a datetime object, if available.
            post_date = (
                datetime.fromtimestamp(job["t_create"])
                if "t_create" in job and job["t_create"]
                else None
            )
        except Exception as e:
            self.logger.error(f"Error parsing job post date: {e}")
            post_date = None

        parsed = {
            'company': self.company_name,
            'job_id': job.get("ats_job_id") or job.get("id"),
            'title': job.get("name"),
            'department': job.get("department"),
            'location': job.get("location"),
            'degree': None,
            'experience_level': None,
            'description': job.get("job_description"),
            'post_date': post_date,
            'scraped_date': datetime.now(),
            'url': job.get("canonicalPositionUrl")
        }

        return parsed