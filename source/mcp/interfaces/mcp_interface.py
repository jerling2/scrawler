from abc import ABC, abstractmethod


class MCPIterface(ABC):

    @abstractmethod
    def setup(self):
        ...

    @abstractmethod
    def teardown(self):
        ...

    @abstractmethod
    def run_loop(self):
        ...
