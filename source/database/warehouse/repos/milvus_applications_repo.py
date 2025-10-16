from typing import Any
from source.database.warehouse.connections import MilvusConnection
from source.database.warehouse.models import MilvusApplicationsModel, MilvusApplicationEntity


class MilvusApplicationsRepo():

    def __init__(
            self, collection_name: str, dim_size: int, chunk_size: int
        ):
        self.collection_name = collection_name
        self.model = MilvusApplicationsModel(dim_size, chunk_size)
        self.conn = MilvusConnection()

    def create_collection(self):
        if self.collection_name in self.conn.collections:
            # Warning: attempted to create a collection that already exists
            return
        schema = self.conn.create_collection_schema(
            self.model.collection_config, 
            self.model.fields
        )        
        index_params = self.conn.create_index_params(
            self.model.indices
        )
        self.conn.client.create_collection(
            collection_name=self.collection_name,
            schema=schema,
            index_params=index_params
        )

    def drop_collection(self):
        if self.collection_name not in self.conn.collections:
            # Warning: Attempted to drop a collection that doesn't exist
            return
        self.conn.client.drop_collection(self.collection_name)

    def insert(self, data: list[MilvusApplicationEntity]) -> dict[str, Any]:
        result = self.conn.client.insert(
            self.collection_name, 
            data=[entity.items for entity in data]
        )
        return result

    def delete(self, application_ids: list[int]) -> dict[str, Any]:
        query = f"application_id in {application_ids}"
        result = self.conn.client.delete(self.collection_name, filter=query)
        return result

    def search(self, vectors: list[float], limit: int) -> list[list[dict[str, Any]]]:
        results = self.conn.client.search(
            self.collection_name,
            data=vectors,
            limit=limit,
            anns_field="chunk_vec",
            output_fields=["application_id", "chunk_idx", "chunk_txt"]
        )
        return [[hit.fields for hit in result] for result in results]
