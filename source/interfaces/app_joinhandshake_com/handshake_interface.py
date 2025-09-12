import json
import asyncio
from pathlib import Path
from source.abstracts import Interface
from .apply.p1 import extract_relevant_jobs
from .apply.p2 import add_job_details
from .apply.p3 import filter_jobs


class Handshake(Interface):
    
    def __init__(self):
        super().__init__()
        with open(Path(__file__).parent / "instructions.json", "r") as f:
            self.instructions = json.load(f)

    @Interface.main_page
    def interact(self):
        OPTIONS_REGISTRY = {
            'apply': self.apply
        }
        options = self.instructions['options']
        if not options:
            raise Exception("Interface.Handshake: requires options")
        selection = self.prompt_options("app.joinhandshake.com (^C to exit)", options, OPTIONS_REGISTRY)
        method_name = options[selection-1]
        method = OPTIONS_REGISTRY.get(method_name, None)
        if not method:
            raise Exception(f"\x1b[1mInterface.Handshake: method {method_name!r} is not registered\x1b[0m")
        method()

    @Interface.sub_page
    def apply(self):
        PARTS_REGISTRY = {
            'extract relevant jobs': self.apply_p1,
            'add job details': self.apply_p2,
            'filter jobs with keywords & llm': self.apply_p3
        }
        parts = self.instructions['apply_parts']
        if not parts:
            raise Exception("Interface.Handshake.apply: requires 'apply_parts'")
        selection = self.prompt_options("app.joinhandshake.com > apply (^C to go back)", parts, PARTS_REGISTRY)
        part_name = parts[selection-1]
        part = PARTS_REGISTRY.get(part_name, None)
        if not part:
            print(f"\x1b[1mInterface.Handshake.apply: part {part_name!r} is not registered\x1b[0m")
            return
        part()

    def apply_p1(self):
        print(self.format_header("app.joinhandshake.com > apply > p1 (^C to go back)"))
        try:
            start = self.prompt_int('\x1b[35;1mEnter starting page number (e.g. 1): \x1b[0m', min_range=1)
            end = self.prompt_int('\x1b[35;1mEnter ending page number (e.g. 20): \x1b[0m', min_range=start)
            per_page = self.prompt_int('\x1b[35;1mEnter jobs per page (e.g. 50): \x1b[0m', min_range=1, max_range=50)
        except KeyboardInterrupt:
            return print()
        asyncio.run(extract_relevant_jobs(start, end, per_page))

    def apply_p2(self):
        print(self.format_header("RUNNING app.joinhandshake.com > apply > p2"))
        asyncio.run(add_job_details())

    def apply_p3(self):
        print(self.format_header("RUNNING app.joinhandshake.com > apply > p3"))
        asyncio.run(filter_jobs())
