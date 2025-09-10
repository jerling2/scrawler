from collections.abc import Callable
from typing import Any, Optional
from pathlib import Path
import csv
import io
import asyncio
import aiofiles


class Writer:

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
  
    def _to_csv(self, serialized_buffer: list[list[str]]) -> str: 
        """
        `csv.writer.writerows` is blocking, so the output is returned as a
        string so it can be written in a thread without blocking the event loop
        """
        output = io.StringIO() #< file-like object in memory (not disk)
        writer = csv.writer(output)
        writer.writerows(serialized_buffer)
        return output.getvalue()

    async def _run(self):
        while self.is_running:
            await self.write_event.wait()
            self.write_event.clear()
            if not len(self.buffer):
                continue
            async with self.write_lock:
                serialized_buffer = self.serialize(self.buffer)
                self.buffer = []
            csv_data = await asyncio.to_thread(self._to_csv, serialized_buffer)
            async with aiofiles.open(self.file, "a", newline="", encoding="utf-8") as f:
                await f.write(csv_data)