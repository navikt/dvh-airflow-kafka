import hashlib
import io
import json
import os
import struct
from typing import Generator, Dict, Text, Any, Tuple, List, Optional
import avro.schema
import avro.io
import requests
import logging
import traceback
from benedict import benedict
from confluent_kafka import Consumer, TopicPartition, Message
from confluent_kafka.error import KafkaError
import environment
from base import Source

_CONFLUENT_SUBJECT_NOT_FOUND = 40401
_CONFLUENT_VERSION_NOT_FOUND = 40402

SchemaCache = Dict[int, avro.io.DatumReader]


class KafkaSource(Source):
    """Kafka Airflow Source"""

    def __init__(self, config: Dict[Text, Any]) -> None:
        super().__init__(config)
        self.value_deserializer = self._set_value_deserializer()

    def _set_value_deserializer(self):
        if self.config["schema"] == "avro":
            value_deserializer = self._avro_deserializer
            self.schema_cache: SchemaCache = {}
        elif self.config["schema"] == "json":
            value_deserializer = self._json_deserializer
        elif self.config["schema"] == "string":
            value_deserializer = self._string_deserializer
        else:
            raise AssertionError

        return value_deserializer

    def set_data_intervals(self) -> None:
        self.data_interval_start: int = int(os.environ["DATA_INTERVAL_START"])
        self.data_interval_end: int = int(os.environ["DATA_INTERVAL_END"])
        logging.info(f"data_interval_start: {self.data_interval_start}")
        logging.info(f"data_interval_stop: {self.data_interval_end}")

    @staticmethod
    def _key_deserializer(x: Optional[bytes]) -> Text:
        if x is None:
            return ""
        return x.decode("utf-8")

    def _json_deserializer(self, message_value: bytes) -> Dict[Text, Any]:
        if message_value is None:
            return benedict(dict(kafka_hash=None, kafka_message=None))
        message = json.loads(message_value.decode("UTF-8"))

        keypath_seperator = self.config.get("keypath-seperator")
        dictionary = benedict(message, keypath_separator=keypath_seperator)

        filter_config = self.config.get("message-fields-filter", [])
        dictionary.remove(filter_config)

        kafka_hash = hashlib.sha256(message_value).hexdigest()
        kafka_message = json.dumps(
            dictionary, ensure_ascii=False)

        dictionary["kafka_message"] = kafka_message
        dictionary["kafka_hash"] = kafka_hash
        return dictionary

    @staticmethod
    def _string_deserializer(x: bytes) -> Tuple[Dict[Text, Any], Text]:
        dictionary = dict(
            kafka_message=json.dumps(
                x.decode("UTF-8"), default=str, ensure_ascii=False)
        )
        kafka_hash = hashlib.sha256(x).hexdigest()
        return dictionary, kafka_hash

    def _avro_deserializer(self, msg: Message) -> Dict[Text, Any]:
        schema_id = struct.unpack(">L", msg[1:5])[0]

        if schema_id not in self.schema_cache:
            self.schema_cache[schema_id] = self._load_avro_schema(schema_id)

        reader = io.BytesIO(msg[5:])
        decoder = avro.io.BinaryDecoder(reader)
        value = self.schema_cache[schema_id].read(decoder)
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
        value["kafka_hash"] = hashlib.sha256(msg[5:]).hexdigest()
        return value

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
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "bootstrap.servers": os.environ["KAFKA_BROKERS"],
            "group.id": self.config["group-id"],
        }

        if environment.isNotLocal:
            config.update(
                {
                    "ssl.certificate.pem": os.environ["KAFKA_CERTIFICATE"],
                    "ssl.key.pem": os.environ["KAFKA_PRIVATE_KEY"],
                    "ssl.ca.pem": os.environ["KAFKA_CA"],
                    "security.protocol": "SSL",
                }
            )
        return config

    def seek_to_timestamp(self, ts: int) -> Dict[int, TopicPartition]:
        topic_metadata = self.consumer.list_topics(
        ).topics[self.config["topic"]]

        tp_with_timestamp_as_offset = [
            TopicPartition(topic=self.config["topic"], partition=k, offset=ts)
            for k in topic_metadata.partitions.keys()
        ]

        topic_partitions = self.consumer.offsets_for_times(
            tp_with_timestamp_as_offset)
        return {tp.partition: tp for tp in topic_partitions}

    def unassign_if_assigned(self, consumer: Consumer, tp: TopicPartition) -> None:
        if tp in consumer.assignment():
            consumer.incremental_unassign([tp])

    def collect_message(self, msg: Message) -> Dict[Text, Any]:
        message = {}
        message["kafka_key"] = KafkaSource._key_deserializer(msg.key())
        message["kafka_timestamp"] = msg.timestamp()[1]
        message["kafka_offset"] = msg.offset()
        message["kafka_partition"] = msg.partition()
        message["kafka_topic"] = msg.topic()
        message.update(self.value_deserializer(msg.value()))

        return message

    def _prepare_partitions(
        self,
    ) -> Tuple[Dict[int, TopicPartition], Dict[int, TopicPartition]]:
        self.set_data_intervals()
        offset_starts = self.seek_to_timestamp(self.data_interval_start)
        offset_ends = self.seek_to_timestamp(self.data_interval_end)

        tp_to_assign_start = {}
        tp_to_assign_end = {}
        for tp in offset_starts.values():
            if tp.offset == -1:
                logging.warning(
                    (
                        f"Provided start data_interval_start: "
                        f"{self.data_interval_start}"
                        f"exceeds that of the last message in the partition."
                    )
                )
            else:
                logging.info(
                    f"Partition {tp.partition} "
                    f"is configured to start at offset: {tp.offset}"
                    f"for topic: {tp.topic}"
                )
                tp_to_assign_start[tp.partition] = tp
                tp_to_assign_end[tp.partition] = offset_ends[tp.partition]

        for tp in tp_to_assign_end.values():
            if tp.offset == -1:
                end_offset = self.consumer.get_watermark_offsets(tp)[1] - 1
                tp.offset = end_offset
                logging.info(
                    (
                        f"data_interval_end: {self.data_interval_end} "
                        f"> last message (offset: {tp.offset}) "
                        f"in the partition: {tp.partition}"
                    )
                )
        return tp_to_assign_start, tp_to_assign_end

    def read_polled_batches(self) -> Generator[List[Dict[Text, Any]], None, None]:
        """
        reads messages from topic beginning (or end)
        or from custom offsets (silently ignored for non-existing partitions)
        """
        self.consumer = Consumer(self._kafka_config())
        batch_size = self.config["batch-size"]
        tp_to_assign_start, tp_to_assign_end = self._prepare_partitions()
        topic_partitions = list(tp_to_assign_start.values())
        self.consumer.assign(topic_partitions)
        logging.info(f"Assigned to {self.consumer.assignment()}.")

        # main loop
        batch: List[Dict[Text, Any]] = []
        empty_counter, non_empty_counter = 0, 0
        assignment_count = len(self.consumer.assignment())
        while assignment_count > 0:
            message: Message | None = self.consumer.poll(timeout=10)
            if message is None:
                empty_counter += 1
                continue
            non_empty_counter += 1

            try:
                err: KafkaError | None = message.error()
                if err is not None:  # handle event or error
                    if not err.retriable() or err.fatal():
                        raise err
                    if err.code() == KafkaError._PARTITION_EOF:
                        err_topic = message.topic()
                        err_partition = message.partition()
                        assert (
                            err_topic is not None
                        ), "Topic missing in EOF sentinel object"
                        assert (
                            err_partition is not None
                        ), "Partition missing in EOF sentinel object"
                        self.consumer.incremental_unassign(
                            [TopicPartition(err_topic, err_partition)]
                        )
                        assignment_count -= 1
                    else:
                        logging.error(err.str())
                elif message.offset() > tp_to_assign_end[message.partition()].offset:
                    # We are at the end
                    self.consumer.incremental_unassign(
                        [TopicPartition(message.topic(), message.partition())]
                    )
                    assignment_count -= 1
                    logging.info(
                        f"partition ({message.partition()}) unassigned at offset ({message.offset()})")

                else:  # handle proper message
                    record = self.collect_message(message)
                    batch.append(record)
                if (len(batch) >= batch_size or assignment_count == 0) and len(
                    batch
                ) > 0:
                    logging.info("Yielding kafka batch.")

                    yield batch
                    batch = []
            except Exception as exc:
                self.consumer.close()
                error_message = "Bailing out..."
                if batch:
                    error_message = error_message + \
                        f", after writing all {len(batch)} messages in batch."
                    yield batch
                else:
                    error_message = error_message + ", no messages read."

                logging.error(error_message)
                raise exc

        self.consumer.close()
        logging.info(f"Completed with {non_empty_counter} events consumed")
        if empty_counter > 0:
            logging.warning(f"found {empty_counter} empty messages")
