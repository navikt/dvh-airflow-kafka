import hashlib
import io
import json
import os
import struct
import avro.schema
import avro.io
from benedict import benedict
import requests
import logging
from typing import Generator, Dict, Text, Any, Tuple, List, Optional, Set
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord, OffsetAndTimestamp
from kafka.structs import TopicPartition
from base import Source
import environment


class KafkaSource(Source):
    """Kafka Airflow Source"""

    def __init__(self, config: Dict[Text, Any]) -> None:
        super().__init__(config)
        self.data_interval_start: int = int(os.environ["DATA_INTERVAL_START"])
        self.data_interval_end: int = int(os.environ["DATA_INTERVAL_END"])

    connection_class = KafkaConsumer

    def _key_deserializer(x: Optional[bytes]) -> Text:
        if x is None:
            return ""
        return x.decode("utf-8")

    def _json_deserializer(self, message_value: bytes) -> Tuple[Dict[Text, Any], Text]:
        if message_value is None:
            return benedict(dict(kafka_hash=None, kafka_message=None))
        message = json.loads(message_value.decode("UTF-8"))

        keypath_seperator = self.config.get("keypath-seperator")
        dictionary = benedict(message, keypath_separator=keypath_seperator)

        filter_config = self.config.get("message-fields-filter", [])
        dictionary.remove(filter_config)

        kafka_hash = hashlib.sha256(message_value).hexdigest()
        dictionary["kafka_hash"] = kafka_hash
        dictionary["kafka_message"] = json.dumps(dictionary, ensure_ascii=True).encode(
            "UTF-8"
        )
        return dictionary

    def _string_deserializer(x: bytes) -> Tuple[Dict[Text, Any], Text]:
        dictionary = dict(
            kafka_message=json.dumps(x.decode("UTF-8"), default=str, ensure_ascii=False)
        )
        kafka_hash = hashlib.sha256(x).hexdigest()
        return dictionary, kafka_hash

    def _avro_deserializer(
        self, x: bytes, schema_cache=dict()
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

        keypath_seperator = self.config.get("keypath-seperator")
        value = benedict(value, keypath_separator=keypath_seperator)

        filter_config = self.config.get("message-fields-filter", [])
        value.remove(filter_config)

        kafka_hash = hashlib.sha256(x[5:]).hexdigest()
        value["kafka_hash"] = kafka_hash
        value["kafka_message"] = value.encode("UTF-8")
        value["kafka_schema_id"] = schema_id
        return value

    def _kafka_config(self, value_deserializer):
        config = dict(
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            bootstrap_servers=os.environ["KAFKA_BROKERS"].split(","),
            key_deserializer=KafkaSource._key_deserializer,
            value_deserializer=value_deserializer,
        )
        if environment.isNotLocal:
            config.update(
                dict(
                    security_protocol="SSL",
                    ssl_certfile=os.environ["KAFKA_CERTIFICATE_PATH"],
                    ssl_keyfile=os.environ["KAFKA_PRIVATE_KEY_PATH"],
                    ssl_cafile=os.environ["KAFKA_CA_PATH"],
                )
            )
        return config

    def read_batches(self) -> Generator[List[Dict[Text, Any]], None, None]:
        def collect_message(record: ConsumerRecord) -> Dict[Text, Any]:
            message = record.value
            message["kafka_key"] = record.key
            message["kafka_timestamp"] = record.timestamp
            message["kafka_offset"] = record.offset
            message["kafka_partition"] = record.partition
            message["kafka_topic"] = record.topic
            return message

        if self.config["schema"] == "avro":
            value_deserializer = self._avro_deserializer
        elif self.config["schema"] == "json":
            value_deserializer = self._json_deserializer
        elif self.config["schema"] == "string":
            value_deserializer = self._string_deserializer
        else:
            raise AssertionError

        logging.info(f"data_interval_start: {self.data_interval_start}")
        logging.info(f"data_interval_stop: {self.data_interval_end}")

        consumer: KafkaConsumer = KafkaSource.connection_class(
            **self._kafka_config(value_deserializer)
        )
        partitions = consumer.partitions_for_topic(self.config["topic"])
        topic_partitions = {TopicPartition(self.config["topic"], p) for p in partitions}
        consumer.assign(topic_partitions)

        tp_ts_dict: Dict[TopicPartition, int] = dict(
            zip(topic_partitions, [self.data_interval_start] * len(topic_partitions))
        )
        offset_starts: Dict[
            TopicPartition, OffsetAndTimestamp
        ] = consumer.offsets_for_times(tp_ts_dict)
        tp_done: Set[TopicPartition] = set()
        offset_ends: Dict[TopicPartition, int] = consumer.end_offsets(topic_partitions)
        for tp, offset_and_ts in offset_starts.items():
            logging.info(f"last offset for {tp}: {offset_ends.get(tp)}")
            if offset_and_ts is None:
                tp_done.add(tp)
            else:
                logging.info(f"start consuming on offset for {tp}: {offset_and_ts}")
                consumer.seek(tp, offset_and_ts.offset)

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
                offset = msg["kafka_offset"]
                end_offset = offset_ends.get(tp) - 1
                if (
                    tp not in tp_done
                    and msg["kafka_timestamp"] >= self.data_interval_end
                ):
                    tp_done.add(tp)
                    consumer.pause(tp)
                    timestamp = msg["kafka_timestamp"]
                    logging.info(
                        f"TopicPartition: {tp} is done on offset: {offset} with timestamp: {timestamp}"
                    )
                if offset == end_offset:
                    logging.info(
                        f"TopicPartition: {tp} is done on offset: {offset} becaused it reached the end of the partition"
                    )
                    tp_done.add(tp)

            batch_filtered = [
                msg for msg in batch if msg["kafka_timestamp"] < self.data_interval_end
            ]

            if len(batch_filtered) > 0:
                yield batch_filtered

            if tp_done == topic_partitions:
                break
