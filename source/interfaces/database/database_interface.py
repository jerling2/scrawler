from pathlib import Path
import json
import os
import copy
from pymilvus import MilvusClient, DataType
from source.abstracts import Interface


class Database(Interface):
    
    def __init__(self):
        super().__init__()
        self.client = MilvusClient(
            uri=os.getenv("MILVUS_URL"),
            token=os.getenv("MILVUS_TOK")
        )
        with open(Path(__file__).parent / "instructions.json", "r") as f:
            self.instructions = json.load(f)

    def interact(self):
        OPTIONS_REGISTRY = {
            'create collection': self.create_collection,
            'list collections': self.list_collections,
            'describe collection': self.describe_collection,
            'drop collection': self.drop_collection
        }
        options = self.instructions['options']
        if not options:
            raise Exception("Interface.Database: requires options")
        while True:
            selection = self.prompt_options("Database (^C to exit)", options, OPTIONS_REGISTRY)
            method_name = options[selection-1]
            method = OPTIONS_REGISTRY.get(method_name, None)
            if not method:
                print(f"\x1b[1mInterface.Database: method {method_name!r} is not registered\x1b[0m")
                continue
            method()
        
    def create_collection(self):
        collection_schemas = self.instructions['collection_schemas']
        schema_registry = {schema['name'] : schema for schema in collection_schemas}
        available_schemas = [schema['name'] for schema in collection_schemas]
        try:
            selection = self.prompt_options("Create Collection (^C to go back)", available_schemas, schema_registry)
        except KeyboardInterrupt:
            return print()
        collection_schema = copy.deepcopy(schema_registry[available_schemas[selection-1]])
        fields = collection_schema.get('fields', None)
        if not fields:
            raise Exception('Collection Schema requires fields')
        schema = MilvusClient.create_schema()
        for field_name, field_kwargs in fields.items():
            datatype = field_kwargs.get('datatype', None)
            if datatype is None:
                raise Exception(f"Field {field_name!r} is missing datatype")
            if not hasattr(DataType, datatype):
                raise Exception(f"Invalid datatype in field ({field_name!r}): {datatype!r}")
            field_kwargs['datatype'] = DataType[datatype]
            schema.add_field(field_name=field_name, **field_kwargs)
        indices = collection_schema.get('indices', None)
        if not indices:
            raise Exception('Collection Schema requires indices')
        index_params = self.client.prepare_index_params()
        for field_name, index_kwargs in indices.items():
            index_params.add_index(field_name=field_name, **index_kwargs)
        existing_collections = self.client.list_collections()
        user_input, is_override = self.prompt_enter("Enter name for collection: ", existing_collections)
        if is_override:
            self.client.drop_collection(collection_name=user_input)
        self.client.create_collection(
            collection_name=user_input,
            schema=schema,
            index_params=index_params
        )

    def list_collections(self):
        message = "\x1b[36;1m--  Collection Info  --\x1b[0;36m\n"
        for collection in self.client.list_collections():
            message += f"  [*] {collection!r}\n"
        message = message.rstrip()
        print(message + '\x1b[0m')
        return

    def describe_collection(self):
        collections = self.client.list_collections()
        try:
            selection = self.prompt_options("Describe Collection (^C to go back)", collections, collections)
        except KeyboardInterrupt:
            return print()
        collection_name = collections[selection-1]
        description = self.client.describe_collection(collection_name=collection_name)
        print(json.dumps(description, indent=4))

    def drop_collection(self):
        collections = self.client.list_collections()
        try:
            selection = self.prompt_options("Drop Collection (^C to go back)", collections, collections)
        except KeyboardInterrupt:
            return print()
        collection_name = collections[selection-1]
        self.client.drop_collection(collection_name=collection_name)
