from pathlib import Path
from dataclasses import dataclass
from typing import Any
import json
import os
import asyncio
from aiolimiter import AsyncLimiter
from pydantic_ai import Agent
from pydantic_ai.usage import RunUsage
from rich.console import Console
from rich.table import Table
from source.interfaces.app_joinhandshake_com.pages.job_details import deserialize_job_details
from source.utilities import (
    Writer,
    Reader,
)


# -- GPT 4.1 -- #
# MODEL = 'openai:gpt-4.1'
# INPUT_TOKEN_COST = 3.00 / 1e6
# OUTPUT_TOKEN_COST = 12.00 / 1e6
# TPM_LIMIT = 30_000
# RPM_LIMIT = 500

# -- GPT 4.1 Nano -- #
MODEL = 'openai:gpt-4.1-nano'
INPUT_TOKEN_COST = 0.20 / 1e6
OUTPUT_TOKEN_COST = 0.80 / 1e6
TPM_LIMIT = 200_000
RPM_LIMIT = 500

APROX_TOKEN_USUAGE_PER_CALL = 120

SAFETY_FACTOR = .95

RATE_LIMIT = int(min(
    TPM_LIMIT * SAFETY_FACTOR / APROX_TOKEN_USUAGE_PER_CALL,
    RPM_LIMIT * SAFETY_FACTOR
))

BATCH_SIZE = 1000


def serialize(buffer: list[Any]) -> list[str]:
    serialized_buffer = []
    for item in buffer:
        serialized_buffer.append([
            item['job_id'], item['position'], item['url'],
            json.dumps(item['additional_documents']), item['company'],
            json.dumps(item['is_external_application']), item['industry'],
            item['posted_date'], item['deadline'], item['pay'],
            json.dumps(item['is_internship']), item['type'], item['duration'],
            item['location'], json.dumps(item['overview']),
        ])
    return serialized_buffer


@dataclass
class SharedFilterState:
    reject_keywords: list[str]
    accept_keywords: list[str]
    accepted_jobs_writer: Writer
    rejected_jobs_writer: Writer
    api_usage_lock: asyncio.Lock
    api_usage: RunUsage
    api_rate_limiter: AsyncLimiter
    agent: Agent[None, bool]


async def keyword_and_llm_filter(job: dict[str, Any], state: SharedFilterState) -> None:
    position = job.get('position', None)
    if not position:
        return
    position = position.lower()
    if any(keyword in position for keyword in state.reject_keywords):
        await state.rejected_jobs_writer.write([job])
        print(f'\x1b[31m[REJECTED] {position}\x1b[0m')
        return
    if any(keyword in position for keyword in state.accept_keywords):
        await state.accepted_jobs_writer.write([job])
        print(f'\x1b[32m[ACCEPTED] {position}\x1b[0m')
        return
    async with state.api_rate_limiter:
        try:
            llm_result = await state.agent.run(position)
        except Exception as e:
            print(f'\x1b[31mllm_result error: {e}\x1b[0m')
            return
        async with state.api_usage_lock:
            state.api_usage += llm_result.usage()
        if llm_result.output:
            await state.accepted_jobs_writer.write([job])
            print(f'\x1b[32m[ACCEPTED] {position}\x1b[0m')
            return
        else:
            await state.rejected_jobs_writer.write([job])
            print(f'\x1b[31m[REJECTED] {position}\x1b[0m')
            return
    

async def filter_jobs():
    storage = Path(os.getenv("STORAGE")) / "app_joinhandshake_com" / "apply"
    p2_file = storage / 'p2.csv'
    if not p2_file.exists():
        raise Exception(f"add_job_details: {p2_file!r} does not exist. Run part 2 before running part 3.")

    reject_keywords = [
        "it ", "designer", "research", "robotic","business", "java ",
        "mobile ", "digital ", ".net", "teacher", "teaching",
        "instructor", "university", "faculty", "school", "qa",
        "quality", "qc", "hacker", "game", "unity", "sales", "code",
        "quality engineer", "forward deployed", "forward-deployed",
    ]
    accept_keywords = [
        "software", "web", "cloud", "network", "javascript", "python",
        "developer", "full stack", "full-stack", "fullstack",
        "front end", "front-end", "frontend", "reactjs", "react js",
        "node.js", "nodejs", "back end", "devops", "platform",
        "dev ops", "backend", "information technology",
        "computer science", "computer scientist", "computer major",
        "machine learning", "ml", "hpc", "seo", "automation",
        "architect", "application"
    ]
    system_prompt = (
        "ACCEPT roles related to computer science, web development, cloud,"
        "or programming, and REJECT roles relatated non-technical skills,"
        "business, art/design, research, or teaching."
        "Respond with `true` or `false`"
    )
    filter_state = SharedFilterState(
        reject_keywords=reject_keywords,
        accept_keywords=accept_keywords,
        accepted_jobs_writer=Writer(storage / 'p3_accepted_jobs.csv', serialize),
        rejected_jobs_writer=Writer(storage / 'p3_rejected_jobs.csv', serialize),
        api_usage_lock=asyncio.Lock(),
        api_usage=RunUsage(),
        agent=Agent(MODEL, output_type=bool, system_prompt=system_prompt),
        api_rate_limiter=AsyncLimiter(max_rate=RATE_LIMIT, time_period=60)
    )
   
    reader = Reader(p2_file, BATCH_SIZE, deserialize_job_details)
    filter_state.accepted_jobs_writer.start()
    filter_state.rejected_jobs_writer.start()
    async for batch in reader:
        await asyncio.gather(
            *(keyword_and_llm_filter(job, filter_state) for job in batch)
        )
        filter_state.accepted_jobs_writer.flush() 
        filter_state.rejected_jobs_writer.flush() 
    await filter_state.accepted_jobs_writer.close()
    await filter_state.rejected_jobs_writer.close()  

    console = Console()
    table = Table()
    table.add_column("Total", style="black", justify="right", no_wrap=True)
    table.add_column("Value", style="black", justify="right")

    num_rejected_jobs = filter_state.rejected_jobs_writer.get_num_lines_written()
    num_accepted_jobs = filter_state.accepted_jobs_writer.get_num_lines_written()
    num_input_tokens = filter_state.api_usage.input_tokens
    num_output_tokens = filter_state.api_usage.output_tokens
    num_requests = filter_state.api_usage.requests
    avg_tokens_per_request = (num_input_tokens + num_output_tokens) / num_requests
    input_cost = num_input_tokens * INPUT_TOKEN_COST
    output_cost = num_output_tokens * OUTPUT_TOKEN_COST

    table.add_row("Rejected Jobs", str(num_rejected_jobs))
    table.add_row("Accepted Jobs", str(num_accepted_jobs))
    table.add_row("API Requests", str(num_requests))
    table.add_row("Input Tokens", f'{num_input_tokens} (${input_cost:.4f})')
    table.add_row("Output Tokens", f'{num_output_tokens} (${output_cost:.4f})')
    table.add_row("Avg TPR", f'{avg_tokens_per_request:.1f}')
    table.add_row("Cost", f'${input_cost + output_cost:.4f}')

    console = Console()
    console.print(table)
