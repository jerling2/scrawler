"""
Apply pipeline
"""
from .p1 import extract_relevant_jobs
from .p2 import add_job_details
from .p3 import filter_jobs


__all__ = ['extract_relevant_jobs', 'add_job_details', 'filter_jobs']
