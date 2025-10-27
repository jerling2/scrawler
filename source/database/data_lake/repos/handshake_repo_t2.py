from bson.objectid import ObjectId
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from source.database.data_lake.connections import MongoConnection


@dataclass(frozen=True)
class HandshakeRepoT2Model:
    SOURCE = 'handshake'

    @classmethod
    def make_document(cls, created_at: datetime, overview: str,
        posted_at: datetime, apply_by: datetime, documents: list[str],
        company: str, industry: str, role: str, is_internal_apply: bool,
        wage: list[int, int] | None, location_type: list[str],
        location: str, job_type: str, is_internship: bool) -> dict[str, Any]:
        return {
            'source': cls.SOURCE,
            'created_at': created_at,
            'overview': overview,
            'posted_at': posted_at,
            'apply_by': apply_by,   
            'documents': documents,
            'company': company,
            'industry': industry,
            'role': role,
            'is_internal_apply': is_internal_apply,
            'wage': wage,
            'location_type': location_type,
            'location': location,
            'job_type': job_type,
            'is_internship': is_internship,
        }


class HandshakeRepoT2:

    def __init__(self, collection_name: str, conn: MongoConnection) -> None:
        self.collection_name = collection_name
        self.conn = conn
        self.model = HandshakeRepoT2Model

    def insert(self, overview: str, posted_at: datetime, 
        apply_by: datetime, documents: list[str], company: str, 
        industry: str, role: str, is_internal_apply: bool,
        wage: list[int, int] | None, location_type: list[str],
        location: str, job_type: str, is_internship: bool) -> ObjectId:
        collection = self.conn.get_collection(self.collection_name)
        document = self.model.make_document(
            created_at=datetime.now(),
            overview=overview,
            posted_at=posted_at,
            apply_by=apply_by,
            documents=documents,
            company=company,
            industry=industry,
            role=role,
            is_internal_apply=is_internal_apply,
            wage=wage,
            location_type=location_type,
            location=location,
            job_type=job_type,
            is_internship=is_internship
        )
        result = collection.insert_one(document)
        return result.inserted_id