from collections.abc import Callable
from typing import Any
from pathlib import Path
import asyncio
import aiofiles
import csv


class Reader:
    def __init__(self, file: Path, batch_size: int, 
    deserialize: Callable[[list[str]], dict[str, Any]]):
        self.file = file
        self.deserialize = deserialize
        self.batch_size = batch_size

    def _csv_line(self, line: str) -> list[str]:
        """
        csv.reader() is offloaded to its own thread,
        otherwise it will stall the entire event loop.
        """
        return next(csv.reader([line]))

    async def __aiter__(self):
        async with aiofiles.open(self.file, mode='r', encoding='utf-8', newline='') as f:
            batch: list[dict[str, Any]] = []
            async for line in f:
                row = await asyncio.to_thread(self._csv_line, line)
                deserialized_row = self.deserialize(row)
                batch.append(deserialized_row)
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch