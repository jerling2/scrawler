import json
import asyncio
from pathlib import Path
from source.abstracts import Interface
from .apply_p1 import extract_relevant_jobs


class Handshake(Interface):
    
    def __init__(self):
        super().__init__()
        with open(Path(__file__).parent / "instructions.json", "r") as f:
            self.instructions = json.load(f)

    def interact(self):
        OPTIONS_REGISTRY = {
            'mass apply': self.mass_apply
        }
        options = self.instructions['options']
        if not options:
            raise Exception("Interface.Handshake: requires options")
        selection = self.prompt_options("app.joinhandshake.com", options, OPTIONS_REGISTRY)
        method_name = options[selection-1]
        method = OPTIONS_REGISTRY.get(method_name, None)
        if not method:
            raise Exception(f"\x1b[1mInterface.Handshake: method {method_name!r} is not registered\x1b[0m")
        method()

    def mass_apply(self):
        PARTS_REGISTRY = {
            'extract relevant jobs': self.mass_apply_p1
        }
        parts = self.instructions['mass_apply_parts']
        if not parts:
            raise Exception("Interface.Handshake.mass_apply: requires 'mass_apply_parts'")
        while True:
            selection = self.prompt_options("app.joinhandshake.com > Mass Apply (^C to exit)", parts, PARTS_REGISTRY)
            part_name = parts[selection-1]
            part = PARTS_REGISTRY.get(part_name, None)
            if not part:
                print(f"\x1b[1mInterface.Handshake.mass_apply: part {part_name!r} is not registered\x1b[0m")
                continue
            part()

    def mass_apply_p1(self):
        print(self.format_header("app.joinhandshake.com > Mass Apply > p1 (^C to go back)"))
        try:
            start = self.prompt_int('\x1b[35;1mEnter starting page number (e.g. 1): \x1b[0m', min_range=1)
            end = self.prompt_int('\x1b[35;1mEnter ending page number (e.g. 20): \x1b[0m', min_range=start)
            per_page = self.prompt_int('\x1b[35;1mEnter jobs per page (e.g. 50): \x1b[0m', min_range=1, max_range=50)
        except KeyboardInterrupt:
            return print()
        asyncio.run(extract_relevant_jobs(start, end, per_page))