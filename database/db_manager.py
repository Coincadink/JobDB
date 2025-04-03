from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class JobListing(Base):
    __tablename__ = 'job_listings'
        
    id = Column(Integer, primary_key=True)
    company = Column(String(100), nullable=False, index=True)
    job_id = Column(String(100), nullable=False, index=True)
    title = Column(String(200), nullable=False)
    department = Column(String(100))
    location = Column(String(200))
    degree = Column(Text)
    experience_level = Column(String(50))
    description = Column(Text)
    post_date = Column(DateTime)
    first_scraped_date = Column(DateTime)
    last_scraped_date = Column(DateTime)
    url = Column(String(512))
    is_active = Column(Boolean, default=True)
    
    def __repr__(self):
        return f"<JobListing(company='{self.company}', title='{self.title}', location='{self.location}')>"

class DatabaseManager:
    def __init__(self, db_path='sqlite:///database/job_listings.db'):
        self.engine = create_engine(db_path)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
    
    def add_or_update_jobs(self, jobs_data):
        """Add new jobs or update existing ones in the database"""
        session = self.Session()
        new_jobs = []
        updated_jobs = []
        
        try:
            for job_data in jobs_data:
                # Check if job already exists
                existing_job = session.query(JobListing).filter_by(
                    company=job_data['company'],
                    job_id=job_data['job_id']
                ).first()
                
                if existing_job:
                    # Update existing job
                    existing_job.job_id = job_data['job_id']
                    existing_job.title = job_data['title']
                    existing_job.department = job_data['department']
                    existing_job.location = job_data['location']
                    existing_job.degree = job_data['degree']
                    existing_job.experience_level = job_data['experience_level']
                    existing_job.description = job_data['description']
                    existing_job.last_scraped_date = datetime.now()
                    existing_job.url = job_data['url']
                    existing_job.is_active = True
                    updated_jobs.append(existing_job)
                else:
                    # Create new job
                    new_job = JobListing(
                        company=job_data['company'],
                        job_id=job_data['job_id'],
                        title=job_data['title'],
                        department=job_data['department'],
                        location=job_data['location'],
                        degree=job_data['degree'],
                        experience_level=job_data['experience_level'],
                        description=job_data['description'],
                        post_date=datetime.fromisoformat(job_data['post_date']),
                        first_scraped_date=datetime.now(),
                        last_scraped_date=datetime.now(),
                        url=job_data['url'],
                        is_active=True
                    )
                    
                    session.add(new_job)
                    new_jobs.append(new_job)
            
            session.commit()
            return new_jobs, updated_jobs
        
        except Exception as e:
            session.rollback()
            raise e
        
        finally:
            session.close()
    
    def mark_inactive_jobs(self, company, active_job_ids):
        """Mark jobs as inactive if they are no longer listed"""
        session = self.Session()
        
        try:
            # Find all active jobs for this company that are not in the active_job_ids list
            inactive_jobs = session.query(JobListing).filter(
                JobListing.company == company,
                JobListing.is_active == True,
                ~JobListing.job_id.in_(active_job_ids)
            ).all()
            
            for job in inactive_jobs:
                job.is_active = False
                
            session.commit()
            return inactive_jobs
            
        finally:
            session.close()
    
    def get_jobs_by_criteria(self, company=None, is_active=None, days=None):
        """Get jobs filtered by criteria"""
        session = self.Session()
        query = session.query(JobListing)
        
        if company:
            query = query.filter(JobListing.company == company)
        
        if is_active is not None:
            query = query.filter(JobListing.is_active == is_active)
        
        if days:
            cutoff_date = datetime.now() - timedelta(days=days)
            query = query.filter(JobListing.first_scraped_date >= cutoff_date)
        
        return query.all()