from collections.abc import Callable
from typing import Any, Optional
from pathlib import Path
import csv
import asyncio


class Serializer:

    def __init__(self, serialize: Callable[[list[Any]], list[list[str]]], path: Path):
        self.serialize = serialize
        self.file = path
        self.write_event = asyncio.Event()
        self.write_lock = asyncio.Lock()
        self.is_running = False
        self.buffer: list[Any] = []
        self.worker: Optional[asyncio.Task[None]] = None

    def start(self):
        if self.is_running:
            return
        self.file.unlink(missing_ok=True)
        self.file.touch(exist_ok=True)
        self.is_running = True
        self.worker = asyncio.create_task(self._run())

    async def close(self):
        if not self.is_running:
            return
        self.is_running = False
        self.flush()
        await self.worker

    async def write(self, data: list[Any]):
        async with self.write_lock:
            self.buffer.extend(data)
    
    def flush(self):
        self.write_event.set()

    def get_buffer_length(self):
        return len(self.buffer)
  
    async def _run(self):
        while self.is_running:
            await self.write_event.wait()
            self.write_event.clear()
            if not len(self.buffer):
                continue
            async with self.write_lock:
                serialized_buffer = self.serialize(self.buffer)
                self.buffer = []
            with open(self.file, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerows(serialized_buffer)