from datetime import datetime
from pymongo import UpdateOne
from typing import TypedDict
from pydantic import TypeAdapter, ValidationError
from source.database.data_lake.connections import MongoConnection


class EnrichedJobData(TypedDict):
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



class HandshakeLake:

    def __init__(self, collection_name: str, conn: MongoConnection = MongoConnection()) -> None:
        self.collection_name = collection_name
        self.conn = conn
        self._enriched_job_data_ta = TypeAdapter(EnrichedJobData)
       

    def connect(self) -> None:
        self.conn.connect()

    def close(self) -> None:
        self.conn.close()

    def upsert_job_postings(self, entities: list[tuple[int, str, str]]):
        if not entities:
            return
        collection = self.conn.get_collection(self.collection_name)
        operations = []
        for e in entities:
            job_id, role, url = e
            upsert = UpdateOne(
                {'job_id': job_id},
                {
                    '$setOnInsert': {
                        'created_at': datetime.now(),
                        'job_id': job_id
                    },
                    '$set': {
                        'role': role,
                        'url': url
                    }
                },
                upsert=True
            )
            operations.append(upsert)
        try:
            res = collection.bulk_write(operations, ordered=False)
            upserted = [i['index'] for i in res.bulk_api_result['upserted']]
            return upserted
        except Exception as err:
            print(f"An unexpected error occured: {err}")
            return []

    def set_e2_success(self, url: str, is_success: bool):
        collection = self.conn.get_collection(self.collection_name)
        collection.update_one(
            { 'url': url },
            {
                '$set': {
                    'url': url,
                    'e2_success': is_success
                }
            },
            upsert=True
        )

    def upsert_enriched_job_data(self, data: EnrichedJobData):
        try:
            self._enriched_job_data_ta.validate_python(data)
        except ValidationError:
            raise ValueError
        collection = self.conn.get_collection(self.collection_name)
        collection.update_one(
            { 'url': data['url'] },
            {
                '$set': data
            },
            upsert=True
        )