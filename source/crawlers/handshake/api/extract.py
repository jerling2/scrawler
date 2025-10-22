from datetime import datetime
from typing import Any


def get_api_command_handshake_extract_jobs_stage_1() -> dict[str, Any]:
    return \
    {
        'jobId': '{date}_{source}_{entity}'.format(
            date=datetime.now().strftime("%Y%m%dT%H%M%S"),
            source='handshake',
            entity='jobs'
        ),
        'action': 'START_EXTRACTION',
        'targetTopic': 'handshake.raw.jobs.stage1.v1'
    }