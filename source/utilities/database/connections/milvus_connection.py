import os
from typing import Any
from dataclasses import dataclass
from pymilvus import MilvusClient


@dataclass(frozen=True)
class MilvusConfig:
    database: str
    uri: str
    token: str

    @classmethod
    def from_env(cls):
        database = os.environ['SCRAWLER_MILVUS_DATABASE']
        uri: str = "http://localhost:19530"
        token: str = "root:Milvus"
        return cls(database, uri, token)
    

class MilvusConnection():

    def __init__(self, config = MilvusConfig.from_env()):
        self._config = config
        self.client = MilvusClient(
            uri=self._config.uri,
            token=self._config.token
        )

    @property
    def collections(self) -> list[str]:
        return self.client.list_collections()

    def create_collection_schema(
            self, 
            collection_config: dict[str, Any],
            fields: list[dict[str, Any]]
        ):
        collection_schema = self.client.create_schema(**collection_config)
        for field in fields:
            collection_schema.add_field(**field)
        return collection_schema

    def create_index_params(self, indices: list[dict[str, str]]):
        index_params = self.client.prepare_index_params()
        for index in indices:
            index_params.add_index(**index)
        return index_params