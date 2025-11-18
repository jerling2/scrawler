from multiprocessing import Process
import os
import signal
from dataclasses import dataclass, field
from source.mcp.interfaces import MCPIterface
from source.mcp.services import MainControlProgram
from source.mcp.models import (
    MCPHandshakeExtractor1Model,
    MCPHandshakeExtractor2Model,
    MCPHandshakeTransformer1Model,
    MCPHandshakeTransformer2Model
)


@dataclass(frozen=True)
class MCPHandshakeETLModel(MCPIterface):

    ETL_PARTS: list[tuple[str, MainControlProgram]] = field(default_factory=lambda: [
        ('hse1', MainControlProgram(MCPHandshakeExtractor1Model())),
        ('hse2', MainControlProgram(MCPHandshakeExtractor2Model())),
        ('hst1', MainControlProgram(MCPHandshakeTransformer1Model())),
        ('hst2', MainControlProgram(MCPHandshakeTransformer2Model()))
    ])
    
    processes: list[Process] = field(default_factory=list)

    def setup(self):
        for name, part in self.ETL_PARTS:
            process = Process(name=name, target=part.run)
            self.processes.append(process)
            process.start()
            print(f"Process(name='{process.name}' pid={process.pid}) started")

    def teardown(self):
        for process in self.processes:
            try:
                os.kill(process.pid , signal.SIGINT)
                print(f"Interrupt signal sent to Process(name='{process.name}' pid={process.pid})")
            except ProcessLookupError:
                print(f"Process(name='{process.name}' pid={process.pid}) was not found. Perhaps it already exited")

    def run_loop(self):
        for process in self.processes:
            process.join()
            print(f"Process(name='{process.name}' pid={process.pid}) finished")
