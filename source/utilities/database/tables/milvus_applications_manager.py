from dataclasses import dataclass, asdict
from typing import Any
from pymilvus import DataType
from source.utilities.database.logging.logger import get_database_logger, log_errors
from source.utilities.database.base.milvus_database import MilvusDatabase

logger = get_database_logger()


@dataclass
class ApplicationData:
    application_id: int
    chunk_idx: int
    chunk_txt: str
    chunk_vec: list[float]


class MilvusApplicationsManager(MilvusDatabase):

    def __init__(self, collection_name, chunk_size, dim_size):
        super().__init__(
            collection_name=collection_name, 
            chunk_size=chunk_size, 
            dim_size=dim_size
        )

    def create_milvus_schema(self):
        collection_schema = self.client.create_schema(
            auto_id=True,
            enable_dynamic_field=False
        )
        collection_schema.add_field(
            field_name="milvus_id",
            datatype=DataType.INT64,
            is_primary=True
        )
        collection_schema.add_field(
            field_name="application_id",
            datatype=DataType.INT64
        )
        collection_schema.add_field(
            field_name="chunk_idx",
            datatype=DataType.INT64
        )
        collection_schema.add_field(
            field_name="chunk_txt",
            datatype=DataType.VARCHAR,
            max_length=self.chunk_size
        )
        collection_schema.add_field(
            field_name="chunk_vec",
            datatype=DataType.FLOAT_VECTOR,
            dim=self.dim_size
        )
        return collection_schema

    def create_milvus_index(self):
        index_params = self.client.prepare_index_params()
        index_params.add_index(
            field_name="application_id",
            index_type="INVERTED",
            index_name="application_id_inverted"
        )
        index_params.add_index(
            field_name="chunk_vec",
            index_type="AUTOINDEX",
            metric_type="COSINE",
            index_name="chunk_vec_auto"
        )
        return index_params

    @log_errors(operation_name="insert")
    def insert(self, data: list[ApplicationData]) -> dict[str, Any]:
        result = self.client.insert(self.collection_name, data=[asdict(x) for x in data])
        logger.info(f"Inserted {result['insert_count']} rows into Milvus collection: '{self.collection_name}'")
        return result
    
    @log_errors(operation_name="delete")
    def delete(self, application_ids: list[int]) -> dict[str, Any]:
        ids = str(application_ids)
        result = self.client.delete(self.collection_name, filter=f"application_id in {ids}")
        logger.info(f"Deleted Application IDS: {ids}. Total deleted: {result['delete_count']} in collection: '{self.collection_name}'")
        return result
    
    @log_errors(operation_name="search")
    def search(self, vectors: list[float], limit: int) -> list[list[dict[str, Any]]]:
        results = self.client.search(
            self.collection_name, 
            data=vectors,
            limit=limit,
            anns_field="chunk_vec",
            output_fields=["application_id", "chunk_idx", "chunk_txt"],
        )
        return [[hit.fields for hit in result] for result in results]