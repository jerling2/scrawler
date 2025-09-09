from pathlib import Path
import json
from source.abstracts import Interface
from source.interfaces import Database, Handshake

OPTIONS_REGISTRY = {
    "database": Database,
    "app.joinhandshake.com": Handshake
}

class System(Interface):
    def __init__(self):
        super().__init__()

    def interact(self, selection=None):
        with open(Path(__file__).parent / "instructions.json", "r") as f:
            instructions = json.load(f)
        options = instructions['options']
        if not options:
            raise Exception("Interface.System: requires options")
        if selection is not None and (selection < 1 or selection > len(options)):
            raise Exception("Interface.System: selection is out of range")
        if not selection:
            selection = self.prompt_options(
                "Scrawler System", options, OPTIONS_REGISTRY
            )
        interface_name = options[selection-1]
        program = OPTIONS_REGISTRY.get(interface_name, None)
        if not program:
            raise Exception(f"Interface.System: interface {interface_name!r} is not registered")
        program().interact()