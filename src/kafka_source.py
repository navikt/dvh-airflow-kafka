import hashlib
import io
import json
import os
import struct
from dataclasses import dataclass
from typing import Generator, Dict, Text, Any, Tuple, List, Optional
import logging
import re

import requests
import avro.schema
import avro.io
from benedict import benedict
from confluent_kafka import Consumer, TopicPartition, Message, KafkaException
from confluent_kafka.error import KafkaError

from .base import Source
from .config import SchemaType, KeyDecoder

SchemaCache = Dict[int, avro.io.DatumReader]


@dataclass
class ProcessSummary:
    event_count: int = 0
    """Number of events processed (including empty and error events)"""

    data_count: int = 0
    """Number of messages with data points processed"""

    error_count: int = 0
    """Number of non-critical errors encountered"""

    written_to_db_count: int = 0
    """Number of messages successfully written to the target database"""

    committed_to_producer_count: int = 0
    """Number of messages successfully committed to the producer (-1 if polling)"""

    empty_count: int = 0
    """Number of empty messages encountered"""

    non_empty_count: int = 0
    """Number of non-empty messages encountered"""


class KafkaSource(Source):
    """Kafka Airflow Source"""

    def __init__(self, config: Dict[Text, Any]) -> None:
        super().__init__(config)
        self.value_deserializer = self._set_value_deserializer()

    def _set_value_deserializer(self):
        if self.config.schema_type == SchemaType.AVRO:
            value_deserializer = self._avro_deserializer
            self.schema_cache: SchemaCache = {}
        elif self.config.schema_type == SchemaType.JSON:
            value_deserializer = self._json_deserializer
        elif self.config.schema_type == SchemaType.STRING:
            value_deserializer = self._string_deserializer
        else:
            raise AssertionError

        return value_deserializer

    def set_data_intervals(self) -> None:
        self.data_interval_start: int = int(os.environ["DATA_INTERVAL_START"])
        self.data_interval_end: int = int(os.environ["DATA_INTERVAL_END"])
        logging.info(f"data_interval_start: {self.data_interval_start}")
        logging.info(f"data_interval_stop: {self.data_interval_end}")

    def _key_deserializer(self, x: Optional[bytes]) -> Text:
        if x is None:
            return ""
        if self.config.key_decoder == KeyDecoder.INT_64:
            return str(int.from_bytes(x, byteorder="big"))
        elif self.config.key_decoder == KeyDecoder.UTF_8:
            return x.decode("utf-8")
        else:
            raise ValueError(f"Decode: {self.config.key_decoder} not valid. Use utf-8 or int-64")

    @staticmethod
    def clean_config(dictionary: benedict[str, Any], filter_config: list | None, flag_field_config: list | None):
        if filter_config is not None:
            keypaths = dictionary.keypaths(indexes=True, sort=False)

            for key in keypaths:
                cleaned_key = re.sub(r"\[\d+\]", "", key)
                if cleaned_key in filter_config:
                    dictionary.remove(key)

        if flag_field_config is not None:
            keypaths = dictionary.keypaths(indexes=True, sort=False)

            for key in keypaths:
                cleaned_key = re.sub(r"\[\d+\]", "", key)
                if cleaned_key in flag_field_config:
                    dictionary[key] = 1 if dictionary[key] is not None else 0

    def _json_deserializer(self, message_value: bytes) -> Dict[Text, Any]:
        if message_value is None:
            return benedict(dict(kafka_hash=None, kafka_message=None))
        message = json.loads(message_value.decode("UTF-8"))

        keypath_separator = self.config.keypath_separator
        dictionary = benedict(message, keypath_separator=keypath_separator)

        filter_config = self.config.message_fields_filter
        flag_field_config = self.config.flag_field_config
        self.clean_config(dictionary, filter_config, flag_field_config)

        kafka_hash = hashlib.sha256(message_value).hexdigest()
        kafka_message = json.dumps(dictionary, ensure_ascii=False)

        dictionary["kafka_message"] = kafka_message
        dictionary["kafka_hash"] = kafka_hash
        return dictionary

    @staticmethod
    def _string_deserializer(x: bytes) -> Tuple[Dict[Text, Any], Text]:
        dictionary = dict(
            kafka_message=json.dumps(x.decode("UTF-8"), default=str, ensure_ascii=False)
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
        dictionary = benedict(value)

        separator = self.config.keypath_separator
        if separator is not None:
            dictionary.keypath_separator = separator

        filter_config = self.config.message_fields_filter
        flag_field_config = self.config.flag_field_config
        self.clean_config(dictionary, filter_config, flag_field_config)

        dictionary["kafka_message"] = json.dumps(dictionary, default=str, ensure_ascii=False)
        dictionary["kafka_schema_id"] = schema_id
        dictionary["kafka_hash"] = hashlib.sha256(msg[5:]).hexdigest()
        return dictionary

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
            "group.id": self.config.group_id,
        }

        if os.environ.get("ENVIRONMENT", "NOT_LOCAL") != "LOCAL":
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
        topic_metadata = self.consumer.list_topics().topics[self.config.topic]

        tp_with_timestamp_as_offset = [
            TopicPartition(topic=self.config.topic, partition=k, offset=ts)
            for k in topic_metadata.partitions.keys()
        ]

        topic_partitions = self.consumer.offsets_for_times(tp_with_timestamp_as_offset)
        return {tp.partition: tp for tp in topic_partitions}

    def unassign_if_assigned(self, consumer: Consumer, tp: TopicPartition) -> None:
        if tp in consumer.assignment():
            consumer.incremental_unassign([tp])

    def collect_message(self, msg: Message) -> Dict[Text, Any]:
        message = dict(
            kafka_key=self._key_deserializer(msg.key()),
            kafka_timestamp=msg.timestamp()[1],
            kafka_offset=msg.offset(),
            kafka_partition=msg.partition(),
            kafka_topic=msg.topic()
        )
        message.update(self.value_deserializer(msg.value()))

        # Ignore certain messages
        message_filters = self.config.message_filters
        if message_filters:
            valid_message = False
            for msg_filter in message_filters:
                # Keep only these messages
                if msg_filter.key in message and msg_filter.allowed_value == message[msg_filter.key]:
                    valid_message = True
                    break
            if not valid_message:
                message["kafka_message"] = None
        return message

    def _data_interval_to_partition_offsets(
        self,
    ) -> Tuple[Dict[int, TopicPartition], Dict[int, TopicPartition]]:
        """Use the configured data to find the correct offsets per partition

        Returns:
            Map from partition id to TopicPartition with start offsets
            Map from partition id to TopicPartition with end offsets
        """
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
                    f"is configured to start at offset: {tp.offset} "
                    f"for topic: {tp.topic}"
                )
                tp_to_assign_start[tp.partition] = tp
                tp_to_assign_end[tp.partition] = offset_ends[tp.partition]

        for tp in tp_to_assign_end.values():
            if tp.offset == -1:
                # No offset for the end timestamp, set to last message
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

    def _unassign_partition(self, partitions: list[TopicPartition], topic: str, partition: int) -> None:
        tp = TopicPartition(topic=topic, partition=partition)
        ind = partitions.index(tp)
        assert ind >= 0 and tp in self.consumer.assignment()
        self.consumer.incremental_unassign([tp])
        del partitions[ind]

    def _poll(self) -> Optional[Message]:
        """This is only a function to be mocked in tests."""
        return self.consumer.poll(timeout=self.config.poll_timeout)

    def read_polled_batches(self) -> Generator[Tuple[List[Dict[Text, Any]], ProcessSummary], None, None]:
        """Reads messages from topic beginning (or end) or from custom offsets (silently ignored for non-existing partitions)

        Assigns partitions to consumer based on data interval start and end timestamps.
        """
        self.consumer = Consumer(self._kafka_config())
        batch_size = self.config.batch_size
        tp_to_assign_start, tp_to_assign_end = self._data_interval_to_partition_offsets()
        topic_partitions = list(tp_to_assign_start.values())
        self.consumer.assign(topic_partitions)
        logging.info(f"Assigned to %s", self.consumer.assignment())

        # main loop
        batch: List[Dict[Text, Any]] = []
        process_summary = ProcessSummary(committed_to_producer_count=-1)
        assigned_partitions = self.consumer.assignment()
        try:
            while assigned_partitions:
                message: Message | None = self._poll()
                if message is None:
                    process_summary.empty_count += 1
                    # avoid infinite loop with timeouts
                    if process_summary.empty_count >= 10:
                        raise RuntimeError("Exceeded maximum number of polls with timeout. Exiting.")
                    continue
                process_summary.event_count += 1
                process_summary.non_empty_count += 1

                err: KafkaError | None = message.error()
                if err is not None:  # handle event or error
                    if not err.retriable() or err.fatal():
                        raise KafkaException(err)
                    elif err.code() == KafkaError._PARTITION_EOF:
                        err_topic = message.topic()
                        err_partition = message.partition()
                        assert err_topic is not None, "Topic missing in EOF sentinel object"
                        assert err_partition is not None, "Partition missing in EOF sentinel object"
                        self._unassign_partition(assigned_partitions, err_topic, err_partition)

                        logging.error(f"Message returned non-critical error: %s", {err})
                        process_summary.error_count += 1

                        # fall through to check if we need to yield batch in case all partitions are done
                    else:
                        logging.error(f"Message returned non-critical error: %s", {err})
                        process_summary.error_count += 1

                else:  # handle proper message
                    record = self.collect_message(message)
                    batch.append(record)
                    process_summary.data_count += 1

                    # Check if message is the last one, if so unassign
                    if message.offset() >= tp_to_assign_end[message.partition()].offset:
                        # We are at the end
                        self._unassign_partition(assigned_partitions, message.topic(), message.partition())

                        logging.info(
                            f"partition ({message.partition()})"
                            f" unassigned at offset ({message.offset()})"
                        )

                if (len(batch) >= batch_size or not assigned_partitions) and len(batch) > 0:
                    logging.info("Yielding kafka batch.")

                    yield batch, process_summary
                    process_summary.written_to_db_count += len(batch)
                    batch = []
        except Exception as exc:
            process_summary.error_count += 1
            if batch:
                error_message = f"Bailing out..., after writing all {len(batch)} messages in batch. Processed: {process_summary}"
                yield batch, process_summary
                process_summary.written_to_db_count += len(batch)
            else:
                error_message = f"Bailing out..., no messages read. Processed: {process_summary}"

            logging.error(error_message)
            raise exc

        finally:
            self.consumer.close()

    def read_subscribed_batches(self) -> Generator[Tuple[List[Dict[Text, Any]], ProcessSummary], None, None]:
        """Reads messages from topic by subscribing to it."""
        process_summary = ProcessSummary()
        self.consumer = Consumer(self._kafka_config())
        self.consumer.subscribe([self.config.topic])
        batch = []
        try:
            while True:
                m = self._poll()

                if m is None:  # No messages
                    logging.info("End of kafka log. Exiting")
                    break

                process_summary.event_count += 1
                process_summary.non_empty_count += 1

                err: KafkaError | None = m.error()
                if err:
                    if not err.retriable() or err.fatal():
                        raise KafkaException(err)
                    logging.error(f"Message returned non-critical error: %s", {err})
                    process_summary.error_count += 1

                else:  # Handle proper message
                    batch.append(self.collect_message(msg=m))
                    process_summary.data_count += 1

                if len(batch) == self.config.batch_size:
                    yield batch, process_summary
                    process_summary.written_to_db_count += len(batch)
                    self.subscribe_commit(self.consumer)
                    process_summary.committed_to_producer_count += len(batch)

                    batch = []
        except Exception as exc:
            process_summary.error_count += 1
            if batch:
                error_message = f"Bailing out..., after writing all {len(batch)} messages in batch. Processed: {process_summary}"
                # handled in finally block
            else:
                error_message = f"Bailing out..., no messages read. Processed: {process_summary}"

            logging.error(error_message)
            raise exc
        finally:
            if batch:
                yield batch, process_summary
                process_summary.written_to_db_count += len(batch)
                self.subscribe_commit(self.consumer)
                process_summary.committed_to_producer_count += len(batch)

            self.consumer.close()

    @staticmethod
    def subscribe_commit(consumer: Consumer):
        resp = consumer.commit(asynchronous=False)

        logging.info(
            f"Committed offsets: {",".join([f"Partition {tp.partition} offset {tp.offset} " for tp in resp])}"
        )
        logging.info(f"Commit response: %s", resp)