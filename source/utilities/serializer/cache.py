from functools import wraps
from dataclasses import dataclass
from typing import Optional, Any, TypeAlias
from pathlib import Path
from .reader import Reader
from .writer import Writer


Query: TypeAlias = tuple[str]


class Cache:
    def __init__(self, reader: Optional[Reader] = None, writer: Optional[Writer] = None):
        self.cache_read = reader
        self.cache_write = writer

    def set_reader(self, reader: Reader):
        self.cache_read = reader

    def set_writer(self, writer: Writer):
        self.cache_write = writer

    @staticmethod
    def requires_reader(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.cache_read is None:
                raise Exception(f"{func.__name__}: The cache is not assigned a Reader")
            return func(self, *args, **kwargs)
        return wrapper

    @staticmethod
    def requires_writer(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.cache_write is None:
                raise Exception(f"{func.__name__}: The cache is not assigned a Writer")
            return func(self, *args, **kwargs)
        return wrapper

    @requires_reader
    async def query_cache(self, writer: Writer, fields: list[str], queries: list[Query]) -> list[Query]:
        writer.start()
        seen = set()
        async for batch in self.cache_read:
            hits = []
            for cache_row in batch:
                selected = tuple(cache_row[field] for field in fields if field in cache_row)
                if selected in queries:
                    seen.add(selected)
                    hits.append(cache_row)
            await writer.write(hits)
        await writer.close()
        misses = []
        for query in queries:
            if query not in seen:
                misses.append(query)
        return misses

    @requires_writer
    def start_write(self):
        self.cache_write.start(overwrite=False)

    @requires_writer
    async def write(self, data: list[Any]):
        await self.cache_write.write(data)

    @requires_writer
    async def close_write(self):
        await self.cache_write.close()
