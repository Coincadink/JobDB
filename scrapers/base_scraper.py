from abc import ABC, abstractmethod
import logging

import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


class Scraper(ABC):
    def __init__(self, company_name):
        self.company_name = company_name
        self.logger = logging.getLogger(f"{self.company_name}Scraper")

    def get_soup(self, url, use_selenium=False):
        """Get BeautifulSoup object from a URL"""
        if use_selenium:
            html = self._get_html_selenium(url)
        else:
            html = self._get_html_requests(url)

        return BeautifulSoup(html, "html.parser")

    def _get_html_requests(self, url):
        """Get HTML using requests"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text

    def _get_html_selenium(self, url, wait_time=10):
        """Get HTML using selenium (for JavaScript-heavy sites)"""
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        try:
            driver.get(url)
            # Give JavaScript time to load content
            driver.implicitly_wait(wait_time)
            return driver.page_source
        finally:
            driver.quit()

    @abstractmethod
    def scrape_jobs(self):
        """Scrape all jobs from listings"""
        pass

    @abstractmethod
    def parse_job(self, job_url):
        """Parse job details from job site"""
        pass