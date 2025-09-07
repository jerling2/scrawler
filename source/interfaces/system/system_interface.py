from pathlib import Path
import json
from source.abstracts import Interface

INTERFACE_REGISTRY = {

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
        options_prompt = "\x1b[36;1m--  Scrawler System --\x1b[0m\n"
        for i, option in enumerate(options):
            options_prompt += "  "                                #< Add indent
            if option not in INTERFACE_REGISTRY:
                options_prompt += "\x1b[31;9m"           #< Red + strikethrough
            else:
                options_prompt += "\x1b[36m"            #< Cyan (if registered)
            options_prompt += f"[{i + 1}] {option}\n"
            options_prompt += "\x1b[0m"
        options_prompt += "\x1b[36mHello! Please select a mode: \x1b[0m"
        if not selection:
            selection = self.prompt_int(options_prompt, 1, len(options))
        interface_name = options[selection-1]
        program = INTERFACE_REGISTRY.get(interface_name, None)
        if not program:
            raise Exception(f"Interface.System: interface {interface_name!r} is not registered")
        program.interact()