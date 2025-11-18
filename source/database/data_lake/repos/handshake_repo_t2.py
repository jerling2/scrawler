from bson.objectid import ObjectId
from dataclasses import dataclass
from typing import TypedDict
from datetime import datetime
from pydantic import TypeAdapter, ValidationError
from source.database.data_lake.connections import MongoConnection


@dataclass(frozen=True)
class HandshakeRepoT2Model:

    class T2RepoData(TypedDict):
        about: str | None
        apply_by: datetime | None
        apply_type: str | None
        company: str | None
        documents: list[str]
        employment_type: str | None
        industry: str | None
        job_type: str | None
        location: str | None
        location_type: list[str]
        position: str | None
        posted_at: datetime | None
        wage: list[int] | None
        url: str

    TA = TypeAdapter(T2RepoData)

    @classmethod
    def validate(cls, t2_repo_data: T2RepoData) -> T2RepoData:
        try:
            return cls.TA.validate_python(t2_repo_data)
        except ValidationError:
            raise ValueError


class HandshakeRepoT2:

    def __init__(self, collection_name: str, conn: MongoConnection = MongoConnection()) -> None:
        self.collection_name = collection_name
        self.conn = conn
        self.model = HandshakeRepoT2Model

    def connect(self) -> None:
        self.conn.connect()

    def close(self) -> None:
        self.conn.close()

    def insert(self, t2_data: HandshakeRepoT2Model.T2RepoData) -> ObjectId:
        collection = self.conn.get_collection(self.collection_name)
        result = collection.insert_one({
            **self.model.validate(t2_data),
            'created_at': datetime.now()
        })
        return result.inserted_id