import zlib
from bson.objectid import ObjectId
from dataclasses import dataclass
from datetime import datetime
from source.utilities import zlib_compress, zlib_decompress
from source.database.data_lake.connections import MongoConnection
from source.database.data_lake.models import HandshakeRawJobListingsModel, HandshakeRawJobListingsEntity


class HandshakeRawJobListingsRepo:

    def __init__(self, collection_name: str, conn: MongoConnection) -> None:
        self.collection_name = collection_name
        self.conn = conn
        self.model = HandshakeRawJobListingsModel()

    def insert_raw_job_listings(self, url: str, html: str) -> ObjectId:
        collection = self.conn.get_collection(self.collection_name)
        document = self.model.make_document(
            created_at=datetime.now(),
            url=url,
            html_payload=zlib_compress(html)
        )
        result = collection.insert_one(document)
        return result.inserted_id
