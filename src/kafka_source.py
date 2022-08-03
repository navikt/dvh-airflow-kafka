import hashlib
import io
import json
import os
import struct
import avro.schema
import avro.io
import requests
from typing import Generator, Dict, Text, Any, Tuple, List, Optional, Set
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord, OffsetAndTimestamp
from kafka.structs import TopicPartition
from base import Source


class KafkaSource(Source):
    """Kafka Airflow Source"""

    connection_class = KafkaConsumer

    @staticmethod
    def _key_deserializer(x: Optional[bytes]) -> Text:
        if x is None:
            return ""
        return x.decode("utf-8")

    @staticmethod
    def _json_deserializer(x: bytes) -> Tuple[Dict[Text, Any], Text]:
        dictionary = json.loads(x.decode("UTF-8"))
        kafka_hash = hashlib.sha256(x).hexdigest()
        dictionary["kafka_message"] = json.dumps(dictionary, default=str)
        return dictionary, kafka_hash

    @staticmethod
    def _string_deserializer(x: bytes) -> Tuple[Dict[Text, Any], Text]:
        dictionary = dict(kafka_message=json.dumps(x.decode("UTF-8"), default=str))
        kafka_hash = hashlib.sha256(x).hexdigest()
        return dictionary, kafka_hash

    @staticmethod
    def _avro_deserializer(
        x: bytes, schema_cache=dict()
    ) -> Tuple[Dict[Text, Any], Text]:
        schema_id = struct.unpack(">L", x[1:5])[
            0
        ]  # Confluence schema 0 xxxx mmmmmmmm..., x:schema id byte, m:message byte

        if not schema_id in schema_cache:
            schema_registry = os.environ["KAFKA_SCHEMA_REGISTRY"]
            un = os.environ["KAFKA_SCHEMA_REGISTRY_USER"]
            pw = os.environ["KAFKA_SCHEMA_REGISTRY_PASSWORD"]
            json_string = requests.get(
                schema_registry + "/schemas/ids/" + str(schema_id), auth=(un, pw)
            ).json()["schema"]
            reader_schema = avro.schema.parse(json_string)
            reader = avro.io.DatumReader(reader_schema)
            schema_cache[schema_id] = reader

        reader = io.BytesIO(x[5:])
        decoder = avro.io.BinaryDecoder(reader)
        value = schema_cache[schema_id].read(decoder)
        value["kafka_message"] = json.dumps(value, default=str)
        value["kafka_schema_id"] = schema_id
        kafka_hash = hashlib.sha256(x[5:]).hexdigest()
        return value, kafka_hash

    def read_batches(self) -> Generator[List[Dict[Text, Any]], None, None]:
        def collect_message(record: ConsumerRecord) -> Dict[Text, Any]:
            message, hash = record.value
            message["kafka_hash"] = hash
            message["kafka_key"] = record.key
            message["kafka_timestamp"] = record.timestamp
            message["kafka_offset"] = record.offset
            message["kafka_partition"] = record.partition
            message["kafka_topic"] = record.topic
            return message

        if self.config["schema"] == "avro":
            value_deserializer = KafkaSource._avro_deserializer
        elif self.config["schema"] == "json":
            value_deserializer = KafkaSource._json_deserializer
        elif self.config["schema"] == "string":
            value_deserializer = KafkaSource._string_deserializer
        else:
            raise AssertionError

        # kafka.KafkaConsumer
        consumer: KafkaConsumer = KafkaSource.connection_class(
            security_protocol="SSL",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            bootstrap_servers=os.environ["KAFKA_BROKERS"].split(","),
            ssl_certfile=os.environ["KAFKA_CERTIFICATE_PATH"],
            ssl_keyfile=os.environ["KAFKA_PRIVATE_KEY_PATH"],
            ssl_cafile=os.environ["KAFKA_CA_PATH"],
            key_deserializer=KafkaSource._key_deserializer,
            value_deserializer=value_deserializer,
        )
        partitions = consumer.partitions_for_topic(self.config["topic"])
        topic_partitions = [TopicPartition(self.config["topic"], p) for p in partitions]
        consumer.assign(topic_partitions)

        ts_start: int = int(os.environ["KAFKA_TIMESTAMP_START"])
        ts_stop: int = int(os.environ["KAFKA_TIMESTAMP_STOP"])

        tp_set: Set[TopicPartition] = consumer.assignment()
        tp_ts_dict: Dict[TopicPartition, int] = dict(
            zip(tp_set, [ts_start] * len(tp_set))
        )
        offset_starts: Dict[
            TopicPartition, OffsetAndTimestamp
        ] = consumer.offsets_for_times(tp_ts_dict)

        for tp, offset_and_ts in offset_starts.items():
            consumer.seek(tp, offset_and_ts.offset)

        tp_done: Set[TopicPartition] = set()

        while True:
            tpd_batch = consumer.poll(
                self.config["batch-interval"], max_records=self.config["batch-size"]
            )
            batch: List[Dict] = [
                collect_message(record)
                for records in tpd_batch.values()
                for record in records
            ]

            for msg in batch:
                tp: TopicPartition = TopicPartition(
                    msg["kafka_topic"], msg["kafka_partition"]
                )
                if msg["kafka_timestamp"] >= ts_stop:
                    tp_done.add(tp)
            batch_filtered = [
                msg for msg in batch
                if TopicPartition(msg["kafka_topic"], msg["kafka_partition"])
                not in tp_done
            ]

            if len(batch_filtered) > 0:
                yield batch_filtered

            if tp_done == tp_set:
                break
