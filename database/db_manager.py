import os
from datetime import datetime
from sqlalchemy import create_engine, Column, String, DateTime, Integer, Text, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Tuple, Any, Optional

Base = declarative_base()

class JobListing(Base):
    """SQLAlchemy model for job listings"""
    __tablename__ = 'job_listings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(255), unique=True, nullable=False, index=True)
    company = Column(String(255), nullable=False, index=True)
    title = Column(String(255), nullable=False)
    department = Column(Text)
    location = Column(Text)
    degree = Column(Text)
    experience_level = Column(Text)
    description = Column(Text)
    post_date = Column(DateTime)
    scraped_date = Column(DateTime, default=datetime.now)
    url = Column(String(512))
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    def __repr__(self):
        return f"<JobListing(id={self.id}, job_id={self.job_id}, company={self.company}, title={self.title})>"


class DatabaseManager:
    """Manages job listings in a SQLite database"""
    
    def __init__(self, db_path: str = "jobs.db"):
        """
        Initialize the database manager
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}")
        self.Session = sessionmaker(bind=self.engine)
        
        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)
    
    def _job_exists(self, session, job_id: str) -> bool:
        """Check if a job with the given job_id exists in the database"""
        return session.query(JobListing).filter_by(job_id=job_id).first() is not None
    
    def _convert_job_data(self, job_data: Dict) -> Dict:
        """Convert job data to appropriate types for database storage"""
        job_dict = job_data.copy()
        
        # Convert post_date to datetime if it's a string
        if isinstance(job_dict.get('post_date'), str):
            try:
                job_dict['post_date'] = datetime.fromisoformat(job_dict['post_date'])
            except (ValueError, TypeError):
                # Fallback if date format is unknown
                job_dict['post_date'] = datetime.now()
        
        # Convert list fields to string representation
        for field in ['department', 'location', 'degree', 'experience_level']:
            if isinstance(job_dict.get(field), list):
                job_dict[field] = ', '.join(str(item) for item in job_dict[field])
        
        return job_dict
    
    def add_job(self, job_data: Dict) -> JobListing:
        """
        Add a single job to the database
        
        Args:
            job_data: Dictionary containing job information
            
        Returns:
            The created JobListing object
        """
        session = self.Session()
        try:
            job_dict = self._convert_job_data(job_data)
            job = JobListing(**job_dict)
            session.add(job)
            session.commit()
            return job
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def update_job(self, job_id: str, job_data: Dict) -> JobListing:
        """
        Update an existing job in the database
        
        Args:
            job_id: The job_id of the job to update
            job_data: Updated job information
            
        Returns:
            The updated JobListing object
        """
        session = self.Session()
        try:
            job = session.query(JobListing).filter_by(job_id=job_id).first()
            if not job:
                raise ValueError(f"Job with job_id {job_id} not found")
            
            job_dict = self._convert_job_data(job_data)
            
            # Update fields
            for key, value in job_dict.items():
                if hasattr(job, key):
                    setattr(job, key, value)
            
            job.last_updated = datetime.now()
            session.commit()
            return job
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def add_or_update_jobs(self, jobs_data: List[Dict]) -> Tuple[List[str], List[str]]:
        """
        Add new jobs or update existing ones in the database
        
        Args:
            jobs_data: List of dictionaries containing job information
            
        Returns:
            Tuple containing (list of new job_ids added, list of existing job_ids updated)
        """
        new_jobs = []
        existing_jobs = []
        
        session = self.Session()
        try:
            for job_data in jobs_data:
                job_id = job_data.get('job_id')
                if not job_id:
                    continue
                
                job_dict = self._convert_job_data(job_data)
                
                # Check if job exists
                existing_job = session.query(JobListing).filter_by(job_id=job_id).first()
                
                if existing_job:
                    # Update existing job
                    for key, value in job_dict.items():
                        if hasattr(existing_job, key):
                            setattr(existing_job, key, value)
                    existing_job.last_updated = datetime.now()
                    existing_jobs.append(job_id)
                else:
                    # Add new job
                    new_job = JobListing(**job_dict)
                    session.add(new_job)
                    new_jobs.append(job_id)
            
            session.commit()
            return new_jobs, existing_jobs
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def get_job_by_id(self, job_id: str) -> Optional[Dict]:
        """
        Retrieve a job by its job_id
        
        Args:
            job_id: The job_id to look up
            
        Returns:
            Dictionary with job data or None if not found
        """
        session = self.Session()
        try:
            job = session.query(JobListing).filter_by(job_id=job_id).first()
            if not job:
                return None
            
            # Convert SQLAlchemy model to dictionary
            return {c.key: getattr(job, c.key) 
                   for c in inspect(job).mapper.column_attrs}
        finally:
            session.close()
    
    def search_jobs(self, 
                   company=None, 
                   title=None, 
                   location=None, 
                   posted_after=None,
                   limit=100) -> List[Dict]:
        """
        Search for jobs based on criteria
        
        Args:
            company: Filter by company name (partial match)
            title: Filter by job title (partial match)
            location: Filter by job location (partial match)
            posted_after: Filter jobs posted after this datetime
            limit: Maximum number of results to return
            
        Returns:
            List of job dictionaries matching criteria
        """
        session = self.Session()
        try:
            query = session.query(JobListing)
            
            if company:
                query = query.filter(JobListing.company.like(f"%{company}%"))
            if title:
                query = query.filter(JobListing.title.like(f"%{title}%"))
            if location:
                query = query.filter(JobListing.location.like(f"%{location}%"))
            if posted_after:
                query = query.filter(JobListing.post_date >= posted_after)
            
            jobs = query.order_by(JobListing.post_date.desc()).limit(limit).all()
            
            return [{c.key: getattr(job, c.key) 
                    for c in inspect(job).mapper.column_attrs}
                   for job in jobs]
        finally:
            session.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the job database
        
        Returns:
            Dictionary with statistics
        """
        session = self.Session()
        try:
            total_jobs = session.query(JobListing).count()
            companies = session.query(JobListing.company, 
                                     func.count(JobListing.id)
                                    ).group_by(JobListing.company).all()
            
            newest_job = session.query(JobListing).order_by(
                JobListing.post_date.desc()).first()
            
            return {
                "total_jobs": total_jobs,
                "companies": {company: count for company, count in companies},
                "newest_job_date": newest_job.post_date if newest_job else None
            }
        finally:
            session.close()