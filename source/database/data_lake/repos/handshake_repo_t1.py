from bson.objectid import ObjectId
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from source.database.data_lake.connections import MongoConnection


@dataclass(frozen=True)
class HandshakeRepoT1Model:
    SOURCE = 'handshake'

    @classmethod
    def make_document(cls, created_at: datetime, job_id: int, role: str, url: str):
        return {
            'source': cls.SOURCE,
            'created_at': created_at,
            'job_id': job_id,
            'role': role,
            'url': url
        }


class HandshakeRepoT1:

    def __init__(self, collection_name: str, conn: MongoConnection = MongoConnection()) -> None:
        self.collection_name = collection_name
        self.conn = conn
        self.model = HandshakeRepoT1Model

    def connect(self) -> None:
        self.conn.connect()

    def close(self) -> None:
        self.conn.close()

    def insert(self, job_id: int, role: str, url: str) -> ObjectId:
        collection = self.conn.get_collection(self.collection_name)
        document = self.model.make_document(
            created_at=datetime.now(),
            job_id=job_id,
            role=role,
            url=url 
        )
        result = collection.insert_one(document)
        return result.inserted_id

    def insert_many(self, entities: list[tuple[int, str, str]]) -> list[ObjectId]:
        documents = [
            self.model.make_document(
                created_at=datetime.now(),
                job_id=entity[0],
                role=entity[1],
                url=entity[2] 
            )
            for entity in entities
        ]
        collection = self.conn.get_collection(self.collection_name)
        results = collection.insert_many(documents)
        return results.inserted_ids