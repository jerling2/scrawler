from abc import ABC, abstractmethod


class MCPIterface(ABC):

    @abstractmethod
    def setup(self):
        """ After model initialization but before the run loop """
        ...

    @abstractmethod
    def teardown(self):
        """ Right before termination to free resources """
        ...

    @abstractmethod
    def run_loop(self):
        """ After model initialization and setup """
        ...
