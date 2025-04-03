import logging
import argparse
from datetime import datetime

# Import scrapers
from scrapers.asml_scraper import ASMLScraper

# Import database manager
from database.db_manager import DatabaseManager


def run_scraper(scraper, db_manager):
    logger = logging.getLogger(f"Scraper-{scraper.company_name}")

    try:
        logger.info(f"Starting scraping for {scraper.company_name}")

        # Scrape all jobs
        jobs = scraper.scrape_jobs()
        logger.info(f"Found {len(jobs)} jobs for {scraper.company_name}")

        # Get all job IDs
        job_ids = [job["job_id"] for job in jobs]

        # Add or update jobs in the database
        new_jobs, updated_jobs = db_manager.add_or_update_jobs(jobs)

        # Mark jobs as inactive if they're no longer listed
        inactive_jobs = db_manager.mark_inactive_jobs(scraper.company_name, job_ids)

        logger.info(f"Added {len(new_jobs)} new jobs")
        logger.info(f"Updated {len(updated_jobs)} existing jobs")
        logger.info(f"Marked {len(inactive_jobs)} jobs as inactive")

        return new_jobs

    except Exception as e:
        logger.error(f"Error scraping {scraper.company_name}: {str(e)}", exc_info=True)
        return []


def main():
    """Main entry point for the job scraper application"""
    # Parse cmdline arguments
    parser = argparse.ArgumentParser(description="Job Scraper for Target Companies")
    parser.add_argument(
        "--days", type=int, default=1, help="Number of days to include in report"
    )
    parser.add_argument(
        "--companies",
        nargs="+",
        default=["all"],
        help="Companies to scrape (e.g., asml intel micron)",
    )
    args = parser.parse_args()

    # Initialize logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(
                f"logs/job_scraper_{datetime.now().strftime('%Y%m%d')}.log"
            ),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger("JobScraper")

    # Initialize database
    db_manager = DatabaseManager()

    # Initialize scrapers
    scrapers = {"asml": ASMLScraper()}

    # Determine which scrapers to run
    if "all" in args.companies:
        scrapers_to_run = list(scrapers.values())
    else:
        scrapers_to_run = [
            scrapers[company.lower()]
            for company in args.companies
            if company.lower() in scrapers
        ]

    # Run scrapers
    all_new_jobs = []
    for scraper in scrapers_to_run:
        new_jobs = run_scraper(scraper, db_manager)
        # all_new_jobs.extend(new_jobs)

    logger.info(f"Scraping completed. Found {len(all_new_jobs)} new jobs.")


if __name__ == "__main__":
    main()
