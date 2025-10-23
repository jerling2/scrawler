import json
from bson.objectid import ObjectId
from datetime import datetime
from typing import Any
from source.utilities import zlib_compress
from source.database.data_lake.connections import MongoConnection
from source.database.data_lake.models import StagedHandshakeJobStage1Model


class StagedHandshakeJobStage1Repo:

    def __init__(self, collection_name: str, conn: MongoConnection) -> None:
        self.collection_name = collection_name
        self.conn = conn
        self.model = StagedHandshakeJobStage1Model()

    def insert_object(self, payload: dict[str, Any]) -> ObjectId:
        collection = self.conn.get_collection(self.collection_name)
        document = self.model.make_document(
            created_at=datetime.now(),
            payload=zlib_compress(json.dumps(payload))
        )
        result = collection.insert_one(document)
        return result.inserted_id
