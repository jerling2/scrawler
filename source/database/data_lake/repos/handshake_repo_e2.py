import zlib
from bson.objectid import ObjectId
from dataclasses import dataclass
from datetime import datetime
from source.database.data_lake.connections import MongoConnection


@dataclass(frozen=True)
class HandshakeRepoE2Model:
    CODEC = 'zlib'
    SOURCE = 'handshake'

    @classmethod
    def make_document(cls, created_at: datetime, url: str, payload: bytes):
        return {
            'source': cls.SOURCE,
            'created_at': created_at,
            'url': url,
            'codec': cls.CODEC,
            'payload': payload
        }
    

class HandshakeRepoE2:

    def __init__(self, collection_name: str, conn: MongoConnection) -> None:
        self.collection_name = collection_name
        self.conn = conn

    def insert(self, url: str, html: str) -> ObjectId:
        collection = self.conn.get_collection(self.collection_name)
        document = HandshakeRepoE2Model.make_document(
            created_at=datetime.now(),
            url=url,
            payload=zlib.compress(html.encode('utf-8'))
        )
        result = collection.insert_one(document)
        return result.inserted_id
