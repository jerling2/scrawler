"""
This module provides the milvus database class
"""
import os
from dataclasses import dataclass
from abc import abstractmethod, ABC
from pymilvus import MilvusClient, DataType
from source.utilities.database.logging.logger import get_database_logger, log_errors


logger = get_database_logger()


@dataclass(frozen=True)
class MilvusConfig:
    database: str
    uri: str
    token: str

    @classmethod
    def from_env(cls):
        database = os.getenv('SCRAWLER_MILVUS_DATABASE', None)
        uri: str = "http://localhost:19530"
        token: str = "root:Milvus"

        if database is None:
            raise EnvironmentError("SCRAWLER_MILVUS_DATABASE is not defined")

        return cls(database, uri, token)
        

class MilvusDatabase(ABC):

    @log_errors(operation_name="init_milvus_client")
    def __init__(
            self, collection_name: str, dim_size: int, chunk_size: int,
            config: MilvusConfig=MilvusConfig.from_env()
        ):
        self.__config = config
        self.__collection_name = collection_name
        self.__dim_size = dim_size
        self.__chunk_size = chunk_size
        self.__client = MilvusClient(
            uri=self.config.uri,
            token=self.config.token
        )
        logger.info(f"Connected to '{config.database}' at {config.uri}")
     
    @abstractmethod
    def create_milvus_schema(self):
        """ returns CollectionSchema """

    @abstractmethod
    def create_milvus_index(self):
        """ returns IndexParams """

    @property
    def config(self):
        return self.__config
    
    @property
    def collection_name(self):
        return self.__collection_name
    
    @property
    def chunk_size(self):
        return self.__chunk_size

    @property
    def dim_size(self):
        return self.__dim_size

    @property
    def client(self):
        return self.__client
    
    @property
    def collections(self):
        return self.__client.list_collections()

    @log_errors(operation_name="MILVUS create_collection")
    def create_collection(self):
        """ Adds a collection to the Milvus Database"""
        if self.collection_name in self.collections:
            logger.warning("Attempted to create a collection that already exists")
            return
        collection_schema = self.create_milvus_schema()
        index_params = self.create_milvus_index()
        self.client.create_collection(
            collection_name=self.collection_name,
            schema=collection_schema,
            index_params=index_params
        )
        logger.info(f"Created Milvus collection '{self.collection_name}' in '{self.config.database}'")

    @log_errors(operation_name="MILVUS drop_collection")
    def drop_collection(self):
        if self.collection_name not in self.collections:
            logger.warning("Attempted to drop a collection that doesn't exist")
            return
        self.client.drop_collection(self.collection_name)
        logger.info(f"Dropped Milvus collection '{self.collection_name}' in '{self.config.database}'")
