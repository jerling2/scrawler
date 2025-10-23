import zlib
import json
from bson.objectid import ObjectId
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from source.database.data_lake.connections import MongoConnection


@dataclass(frozen=True)
class HandshakeRepoT1Model:
    SOURCE = 'handshake'

    @classmethod
    def make_document(cls, created_at: datetime, payload: dict[str, Any]):
        return {
            'source': cls.SOURCE,
            'created_at': created_at,
            'payload': payload
        }


class HandshakeRepoT1:

    def __init__(self, collection_name: str, conn: MongoConnection) -> None:
        self.collection_name = collection_name
        self.conn = conn
        self.model = HandshakeRepoT1Model

    def insert(self, payload: dict[str, Any]) -> ObjectId:
        collection = self.conn.get_collection(self.collection_name)
        document = self.model.make_document(
            created_at=datetime.now(),
            payload=zlib.compress(json.dumps(payload).encode('utf-8'))
        )
        result = collection.insert_one(document)
        return result.inserted_id
