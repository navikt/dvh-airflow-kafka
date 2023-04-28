import json
import os
import io
import requests
import kafka
import avro
import hashlib

from typing import Dict, Text, Any, List, Optional, Tuple


class AvroUtils:

    def __init__(self) -> None:
        self.schema_cache = dict()

    def avro_schema(self, schema_id: int = 0, latest: bool = False) -> Text:
        if schema_id not in self.schema_cache:
            schema_registry = os.environ["KAFKA_SCHEMA_REGISTRY"]
            un = os.environ["KAFKA_SCHEMA_REGISTRY_USER"]
            pw = os.environ["KAFKA_SCHEMA_REGISTRY_PASSWORD"]
            json_string = requests.get(
                schema_registry + "/schemas/ids/" + str("latest" if latest else schema_id), auth=(un, pw)
            ).json()["schema"]
            avro_schema = avro.schema.parse(json_string)
            self.schema_cache[schema_id] = avro_schema
        return self.schema_cache[schema_id]

    def avro_serializer(
        message: Text,
        self
    ) -> bytes:
        #Get schema from registry
        schema = self.schema_cache.avro_schema(latest=True)
        writer = avro.io.DatumWriter(schema)

        buf = io.BytesIO()
        encoder = avro.io.BinaryEncoder(buf)
        writer.write(message, encoder)
        buf.seek(0)
        
        return (buf.read())


def key_serializer(x: Optional[Text]) -> bytes:
    if x is None:
        return ""
    return x.encode("utf-8")


def key_deserializer(x: Optional[bytes]) -> Text:
    if x is None:
        return ""
    return x.decode("utf-8")

        
def string_deserializer(x: bytes) -> Tuple[Text, Text]:
    value = x.decode("UTF-8")
    kafka_hash = hashlib.sha256(x).hexdigest()
    return value, kafka_hash


def json_serializer(dictionary: Dict[Text, Any]) -> bytes:
    json_str = json.dumps(dictionary)
    json_bytes = json_str.encode('UTF-8')
    return json_bytes

def json_deserializer(x: bytes) -> Tuple[Dict[Text, Any], Text]:
    dictionary = json.loads(x.decode("UTF-8"))
    kafka_hash = hashlib.sha256(x).hexdigest()
    dictionary["kafka_message"] = json.dumps(dictionary, default=str)
    return dictionary, kafka_hash