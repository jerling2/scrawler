"""
To multi-process the pipeline the requirements are:
1. each step of the pipeline is contained in it's own process.
2. each process is connected to the message highway
3. A mcp should orchastrate the entire thing.


I can make a SUPER MCP that orchastrates smaller MCPs.
"""

class MainETLHandshake:
    
    def __init__(self) -> None:
        pass

    def setup(self) -> None:
        pass

    def teardown(self) -> None:
        pass

    def run_loop(self) -> None:
        # This should launch multiple processes
        pass

