"""
Part one of the app.joinhandshake.com `apply` pipeline.

This module handles the initial job extraction phase by scraping job listings
from the Handshake job search endpoint and saving them to CSV format for
further processing.
"""
import os
from pathlib import Path
from source.interfaces.app_joinhandshake_com import job_search, JobSearchState


async def extract_relevant_jobs(start: int, end: int, per_page: int) -> None:
    """
    Scrape job listings from the Handshake job search endpoint.
    
    Extracts job data from the specified page range and saves the results
    as a CSV file containing job_id, position, and href columns. The output
    is saved to the configured storage directory under the apply pipeline.
    
    Args:
        start (int): Starting page number for job search (inclusive)
        end (int): Ending page number for job search (inclusive) 
        per_page (int): Number of job listings to fetch per page
    
    Returns:
        None: Results are written to 'p1.csv' in the storage directory
        
    Raises:
        EnvironmentError: If STORAGE environment variable is not set
        OSError: If storage directory cannot be created or accessed
    """
    storage = Path(os.getenv("STORAGE")) / "app_joinhandshake_com" / "apply"
    await job_search(
        state=JobSearchState(
            file=storage/'p1.csv',
            start=start,
            end=end,
            per_page=per_page
        )
    )
