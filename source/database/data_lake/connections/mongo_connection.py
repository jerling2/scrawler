import os
from urllib.parse import quote_plus
from dataclasses import dataclass
from pymongo import MongoClient


@dataclass(frozen=True)
class MongoConnectionConfig:
    username: str
    password: str 
    host: str
    port: str
    database: str

    @classmethod
    def from_env(cls):
        username = os.environ['SCRAWLER_MONGO_SUPERUSER_USER']
        password = os.environ['SCRAWLER_MONGO_SUPERUSER_PASS']
        host = os.environ['SCRAWLER_MONGO_HOST']
        port = os.environ['SCRAWLER_MONGO_PORT']
        database = os.environ['SCRAWLER_MONGO_DATABASE']
        return cls(username, password, host, port, database)

    def __post_init__(self):
        # username and password must be escaped according to RFC 3986
        object.__setattr__(self, 'username', quote_plus(self.username))
        object.__setattr__(self, 'password', quote_plus(self.password))

    @property
    def uri(self):
        return \
        (
            f'mongodb://{self.username}:{self.password}'
            f'@{self.host}:{self.port}/{self.database}?authSource=admin'
        )


class MongoConnection:

    def __init__(self, config = MongoConnectionConfig.from_env()):
        self.config = config
        self.client = None

    def connect(self):
        if self.client is None:
            self.client = MongoClient(self.config.uri)
    
    def close(self):
        if self.client is not None:
            self.client.close()
        self.client = None

    def get_database(self):
        if not self.client:
            raise RuntimeError("You must call .connect() before accessing collections.")
        return self.client.get_default_database()
    
    def get_collection(self, collection_name: str):
        if not self.client:
            raise RuntimeError("You must call .connect() before accessing collections.")
        return self.client.get_default_database()[collection_name]
