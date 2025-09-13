import os
from pathlib import Path
from ..pages.job_search import job_search, JobSearchState


async def extract_relevant_jobs(start: int, end: int, per_page: int) -> None:
    storage = Path(os.getenv("STORAGE")) / "app_joinhandshake_com" / "apply"
    await job_search(
        state=JobSearchState(
            file=storage/'p1.csv',
            start=start,
            end=end,
            per_page=per_page
        )
    )