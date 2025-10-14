from dataclasses import dataclass, asdict
from typing import Any
from pymilvus import DataType


@dataclass(frozen=True)
class MilvusApplicationEntity:
    application_id: int
    chunk_idx: int
    chunk_txt: str
    chunk_vec: list[float]

    @property
    def items(self) -> dict[str, Any]:
        return asdict(self)
    

@dataclass(frozen=True)
class MilvusApplicationsModel:
    dim_size: int
    chunk_size: int
    
    def __post_init__(self):
        if type(self.dim_size) != int:
            raise ValueError(f"Expected dim_size to be an int, instead got {type(self.dim_size)}")
        if type(self.chunk_size) != int:
            raise ValueError(f"Expected chunk_size to be an int, instead got {type(self.chunk_size)}")
        
    @property
    def collection_config(self) -> dict[str, Any]:
        config = \
        {
            'auto_id': True,
            'enable_dynamic_field': False
        }
        return config
    
    @property
    def fields(self) -> list[dict[str, Any]]:
        fields = \
        [
            {
                'field_name': "milvus_id",
                'datatype': DataType.INT64,
                'is_primary': True
            },
            {
                'field_name': "application_id",
                'datatype': DataType.INT64
            },
            {
                'field_name': "chunk_idx",
                'datatype': DataType.INT64
            },
            {
                'field_name': "chunk_txt",
                'datatype': DataType.VARCHAR,
                'max_length': self.chunk_size
            },
            {
                'field_name': "chunk_vec",
                'datatype': DataType.FLOAT_VECTOR,
                'dim': self.dim_size
            }
        ]
        return fields

    @property
    def indices(self) -> list[dict[str, str]]:
        indices = \
        [
            {
                'field_name': "application_id",
                'index_type': "INVERTED",
                'index_name': "application_id_inverted"
            },
            {
                'field_name': "chunk_vec",
                'index_type': "AUTOINDEX",
                'metric_type': "COSINE",
                'index_name': "chunk_vec_auto"
            }
        ]
        return indices