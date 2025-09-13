"""
Part two of the app.joinhandshake.com `apply` pipeline.

This module handles the second job extraction phase by scraping additional job
details (e.g. company name, salary, location, job type, position overview, ...)
from the handshake `jobs` endpoint, and saving the results to a CSV file for
further processing.
"""
import os
from pathlib import Path
from source.interfaces.app_joinhandshake_com import (
    deserialize_job_search,
    JobDetailsState,
    job_details
)


async def add_job_details():
    """
    Enrich job listings with detailed information from individual job pages.
    
    Reads the basic job data from part 1 (job_id, position, href) and fetches
    additional details such as company name, salary, location, job type, and
    position overview for each job. The enriched data is saved to a new CSV
    file for the next pipeline stage.

    Args:
        None: Input is read from 'p1.csv' file in the storage directory.

    Returns:
        None: Results are written to 'p2.csv' in the storage directory.

    Raises:
        EnvironmentError: If STORAGE environment variable is not set
        OSError: If storage directory cannot be created or accessed
        FileNotFoundError: If 'p1.csv' does not exist (part 1 must run first)
    """
    storage = Path(os.getenv("STORAGE")) / "app_joinhandshake_com" / "apply"
    p1_file = storage / 'p1.csv'
    if not p1_file.exists():
        raise FileNotFoundError(
            f"add_job_details: {p1_file!r} does not exist. Run part 1 before running part 2."
        )
    p2_file = storage / 'p2.csv'
    await job_details(
        state=JobDetailsState(
            file_input=p1_file,
            file_output=p2_file,
            deserialize=deserialize_job_search
        )
    )
