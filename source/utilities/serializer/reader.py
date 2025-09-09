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

    async def __aiter__(self):
        async with aiofiles.open(self.file, mode='r', encoding='utf-8', newline='') as f:
            batch: list[dict[str, Any]] = []
            async for line in f:
                row = next(csv.reader([line]))
                deserialized_row = self.deserialize(row)
                batch.append(deserialized_row)
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch


""" --- EXAMPLE (REMOVE LATER) --- """
# def deserialize(row: list[str]) -> dict[str, Any]:
#     return {
#         'job_id': row[0],
#         'position': row[1],
#         'url': row[2]
#     }
# async def main():
#     import json
#     file = Path('p1.csv')
#     reader = Reader(Path('p1.csv'), 20, deserialize)
#     async for batch in reader:
#         print(json.dumps(batch, indent=4))
# if __name__ == "__main__":
#     asyncio.run(main())