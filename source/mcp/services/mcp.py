import signal
from source.mcp.interfaces import MCPIterface


class MainControlProgram:

    def __init__(self, model: MCPIterface) -> None:
        self.model = model
        self.signal_handler_context = {}

    def _exit_gracefully(self, signum, frame):
        _ = signum, frame
        self.model.teardown()
        self._restore_signal_handlers()

    def _attach_signal_handlers(self):
        self.signal_handler_context[signal.SIGINT] = \
            signal.signal(signal.SIGINT, self._exit_gracefully)
        self.signal_handler_context[signal.SIGTERM] = \
            signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _restore_signal_handlers(self):
        signal.signal(signal.SIGINT, self.signal_handler_context[signal.SIGINT])
        signal.signal(signal.SIGTERM, self.signal_handler_context[signal.SIGTERM])

    def run(self):
        self._attach_signal_handlers()
        self.model.run_loop()