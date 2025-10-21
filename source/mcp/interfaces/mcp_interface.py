from typing import Protocol


class MCPIterface(Protocol):

    def teardown(self):
        ...

    def run_loop(self):
        ...