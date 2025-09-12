import os
from pathlib import Path
from ..pages.job_search import deserialize as deserialize_p1_file
from ..pages.job_details import JobDetailsState, job_details


async def add_job_details():
    storage = Path(os.getenv("STORAGE")) / "app_joinhandshake_com" / "apply"
    p1_file = storage / 'p1.csv'
    if not p1_file.exists():
        raise Exception(f"add_job_details: {p1_file!r} does not exist. Run part 1 before running part 2.")
    p2_file = storage / 'p2.csv'
    await job_details(
        state=JobDetailsState(
            file_input=p1_file,
            file_output=p2_file,
            deserialize=deserialize_p1_file
        )
    )