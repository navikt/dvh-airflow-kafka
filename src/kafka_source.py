import hashlib
import io
import json
import os
import struct
from typing import Generator, Dict, Text, Any, Tuple, List, Optional, Set

import avro.schema
import avro.io
import requests
import logging
from benedict import benedict
from confluent_kafka import Consumer, TopicPartition, Message

import environment
from base import Source

SchemaCache = Dict[int, avro.io.DatumReader]


class KafkaSource(Source):
    """Kafka Airflow Source"""
    connection_class = Consumer

    def __init__(self, config: Dict[Text, Any]) -> None:
        super().__init__(config)
        self.value_deserializer = self._set_value_deserializer()
        self.data_interval_start: int = int(os.environ["DATA_INTERVAL_START"])
        self.data_interval_end: int = int(os.environ["DATA_INTERVAL_END"])

    def _set_value_deserializer(self):
        if self.config["schema"] == "avro":
            value_deserializer = self._avro_deserializer
        elif self.config["schema"] == "json":
            value_deserializer = self._json_deserializer
        elif self.config["schema"] == "string":
            value_deserializer = self._string_deserializer
        else:
            raise AssertionError

        return value_deserializer

    @staticmethod
    def _key_deserializer(x: Optional[bytes]) -> Text:
        if x is None:
            return ""
        return x.decode("utf-8")

    def _json_deserializer(self, message_value: bytes) -> Tuple[Dict[Text, Any], Text]:
        if message_value is None:
            return benedict(dict(kafka_hash=None, kafka_message=None))
        message = json.loads(message_value.decode("UTF-8"))
        logging.info(message)

        keypath_seperator = self.config.get("keypath-seperator")
        dictionary = benedict(message, keypath_separator=keypath_seperator)

        filter_config = self.config.get("message-fields-filter", [])
        dictionary.remove(filter_config)

        kafka_hash = hashlib.sha256(message_value).hexdigest()
        kafka_message = json.dumps(dictionary, ensure_ascii=True).encode(
            "UTF-8"
        )
        return kafka_hash, kafka_message

    @staticmethod
    def _string_deserializer(x: bytes) -> Tuple[Dict[Text, Any], Text]:
        dictionary = dict(
            kafka_message=json.dumps(
                x.decode("UTF-8"), default=str, ensure_ascii=False)
        )
        kafka_hash = hashlib.sha256(x).hexdigest()
        return dictionary, kafka_hash

    def _avro_deserializer(
            self, msg: Message, schema_cache: SchemaCache = {}
    ) -> Tuple[Dict[Text, Any], Text]:
        schema_id = struct.unpack(">L", msg[1:5])[0]

        if schema_id not in schema_cache:
            schema_cache[schema_id] = self._load_avro_schema(schema_id)

        reader = io.BytesIO(msg[5:])
        decoder = avro.io.BinaryDecoder(reader)
        value = schema_cache[schema_id].read(decoder)
        value = benedict(value)

        separator = self.config.get("keypath-seperator")
        if separator is not None:
            value.keypath_separator = separator

        filter_config = self.config.get("message-fields-filter")
        if filter_config is not None:
            value.remove(filter_config)

        value["kafka_message"] = json.dumps(
            value, default=str, ensure_ascii=False)
        value["kafka_schema_id"] = schema_id
        kafka_hash = hashlib.sha256(msg[5:]).hexdigest()
        return value, kafka_hash

    def _load_avro_schema(self, schema_id: int) -> avro.io.DatumReader:
        schema_registry = os.environ["KAFKA_SCHEMA_REGISTRY"]
        un = os.environ["KAFKA_SCHEMA_REGISTRY_USER"]
        pw = os.environ["KAFKA_SCHEMA_REGISTRY_PASSWORD"]
        json_string = requests.get(
            f"{schema_registry}/schemas/ids/{schema_id}", auth=(un, pw)
        ).json()["schema"]
        reader_schema = avro.schema.parse(json_string)
        return avro.io.DatumReader(reader_schema)

    def _kafka_config(self) -> Dict[Text, Any]:
        config = {
            "group.id": self.config["group-id"],
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "bootstrap.servers": os.environ["KAFKA_BROKERS"],
        }

        if environment.isNotLocal:
            config.update({
                "ssl.certificate.location": os.environ["KAFKA_CERTIFICATE_PATH"],
                "ssl.key.location": os.environ["KAFKA_PRIVATE_KEY_PATH"],
                "ssl.ca.location": os.environ["KAFKA_CA_PATH"],
                "security.protocol": "SSL",
            })
        return config

    def seek_to_timestamp(self, consumer: Consumer, ts: int) -> Dict[int, TopicPartition]:
        topic_metadata = consumer.list_topics().topics[self.config["topic"]]

        tp_with_timestamp_as_offset = [
            TopicPartition(
                topic=self.config["topic"],
                partition=k,
                offset=ts)
            for k in topic_metadata.partitions.keys()
        ]

        topic_partitions = consumer.offsets_for_times(tp_with_timestamp_as_offset)
        return {tp.partition: tp for tp in topic_partitions}

    def unassign_if_assigned(self, consumer: Consumer, tp: TopicPartition) -> None:
        if tp in consumer.assignment():
            consumer.incremental_unassign([tp])

    def collect_message(self, msg: Message) -> Dict[Text, Any]:
        message = {}
        if msg.error():
            error = msg.error()
            error_message = f"Time: {msg.timestamp()}\n Offset: {msg.offset()}\nError code: {error.code()}\nReason: {error.str()}",
            error_message_hash = hashlib.sha256(error_message).hexdigest()
            message["kafka_message"], message["kafka_hash"] = error_message, error_message_hash
        else:
            message["kafka_message"], message["kafka_hash"] = self.value_deserializer(msg.value())
        message["kafka_key"] = KafkaSource._key_deserializer(msg.key())
        message["kafka_timestamp"] = msg.timestamp()[1]
        message["kafka_offset"] = msg.offset()
        message["kafka_partition"] = msg.partition()
        message["kafka_topic"] = msg.topic()
        return message

    def read_batches(self) -> Generator[List[Dict[Text, Any]], None, None]:
        logging.info(f"data_interval_start: {self.data_interval_start}")
        logging.info(f"data_interval_stop: {self.data_interval_end}")

        consumer = KafkaSource.connection_class(self._kafka_config())

        offset_starts = self.seek_to_timestamp(consumer, self.data_interval_start)
        offset_ends = self.seek_to_timestamp(consumer, self.data_interval_end)

        tp_to_assign_start, tp_to_assign_end = self._prepare_partitions(offset_starts, offset_ends, consumer)

        consumer.assign(list(offset_starts.values()))

        while consumer.assignment():
            tpd_batch = consumer.consume(
                num_messages=self.config["batch-size"],
                timeout=self.config["batch-interval"]
            )

            batch: List[Dict] = [self.collect_message(msg) for msg in tpd_batch]
            for msg in batch:
                if msg["kafka_offset"] % 500 == 0:
                    logging.info(f'Current kafka_offset: {msg["kafka_offset"]}')
                tp = TopicPartition(
                    msg["kafka_topic"], msg["kafka_partition"], msg["kafka_offset"]
                )

                end_offset = tp_to_assign_end[tp.partition].offset - 1
                if msg["kafka_timestamp"] >= self.data_interval_end:
                    self.unassign_if_assigned(consumer, tp)
                    logging.info(
                        f"TopicPartition: {tp} is done on offset: {tp.offset} with timestamp: {msg['kafka_timestamp']}"
                    )

                if tp.offset == end_offset:
                    self.unassign_if_assigned(consumer, tp)
                    logging.info(
                        f"TopicPartition: {tp} is done on offset: {tp.offset} because it's reached the end"
                    )

            batch_filtered = [
                msg for msg in batch if msg["kafka_timestamp"] < self.data_interval_end
            ]

            if len(batch_filtered) > 0:
                yield batch_filtered

    def _prepare_partitions(self, offset_starts: Dict[int, TopicPartition], offset_ends: Dict[int, TopicPartition],
                            consumer: Consumer) -> Tuple[Dict[int, TopicPartition], Dict[int, TopicPartition]]:
        tp_to_assign_start = {}
        tp_to_assign_end = {}
        for tp in offset_starts.values():
            if tp.offset == -1:
                logging.warning(
                    f"Provided start data_interval_start: {self.data_interval_start} exceeds that of the last message in the partition.")
            else:
                logging.info(
                    f"Start consuming on offset for {tp.partition}: {tp.offset}")
                tp_to_assign_start[tp.partition] = tp
                tp_to_assign_end[tp.partition] = offset_ends[tp.partition]

        for tp in tp_to_assign_end.values():
            if tp.offset == -1:
                end_offset = consumer.get_watermark_offsets(tp)[1]
                tp.offset = end_offset
                logging.info(
                    f"Provided data_interval_end: {self.data_interval_end} exceeds that of the last message in the partition.")
        return tp_to_assign_start, tp_to_assign_end
