import hashlib
import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable
from unittest.mock import patch, MagicMock

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic

from ..kafka_source import KafkaSource, ProcessSummary
from ..mapping import Mapping
from ..oracle_target import OracleTarget
from ..transform import Transform

TABLE_NAME = "RAA_DATA_STROM"
NOW = datetime(2024, 12, 18, 11, 11, 11)
MAX_NUM_KAFKA_MESSAGES = 1000


def build_config(base_config, topic_name: str, strategy: str) -> dict:
    base_config["source"]["topic"] = topic_name
    base_config["source"]["strategy"] = strategy
    base_config["source"]["group-id"] = topic_name
    base_config["target"]["table"] = TABLE_NAME
    return base_config


def setup_mapping(assign_config, transform_config) -> tuple[OracleTarget, Mapping]:
    os.environ["DATA_INTERVAL_START"] = str(
        int(datetime.timestamp(NOW - timedelta(days=MAX_NUM_KAFKA_MESSAGES)))
    )
    os.environ["DATA_INTERVAL_END"] = str(
        int(
            datetime.timestamp(NOW + timedelta(days=(MAX_NUM_KAFKA_MESSAGES - MAX_NUM_KAFKA_MESSAGES // 2 - 1)))
        )
    )
    kafka_source = KafkaSource(assign_config["source"])
    oracle_target = OracleTarget(assign_config["target"])
    transform = Transform(transform_config)
    return oracle_target, Mapping(kafka_source, oracle_target, transform)

@dataclass
class Row:
    key: str
    offset: int
    partition: int
    timestamp: datetime
    topic: str
    hash: str
    message: str
    lastet_tid: datetime
    kildesystem: str
    object: dict

def get_kafka_messages(oracle_target, topic) -> list[Row]:
    with oracle_target.oracle_connection() as con:
        with con.cursor() as cur:
            table_name = oracle_target.config.table
            cur.execute(f"select kafka_key, kafka_offset, kafka_partition, kafka_timestamp, kafka_topic, kafka_hash, kafka_message, lastet_tid, kildesystem "
                        f"from {table_name} "
                        f"where kafka_topic = :topic order by kafka_timestamp",
                        topic=topic)

            rows = []
            for row in cur.fetchall():
                rows.append(Row(
                    key=row[0],
                    offset=row[1],
                    partition=row[2],
                    timestamp=row[3],
                    topic=row[4],
                    hash=row[5],
                    message=row[6].read(),
                    lastet_tid=row[7],
                    kildesystem=row[8],
                    object=json.loads(row[6].read())
                ))

            return rows


def create_topic(kafka_admin_client, topic_name: str, num_partitions: int = 1):
    futures = kafka_admin_client.create_topics(
        [NewTopic(topic=topic_name, num_partitions=num_partitions)]
    )
    futures[topic_name].result()

def get_kafka_timestamp(message_offset: int) -> int:
    return int(datetime.timestamp(NOW - timedelta(days=(MAX_NUM_KAFKA_MESSAGES - message_offset - 1))))

def produce_default_message(producer: Producer, topic: str, i: int, partition: int = None):
    producer.produce(
        topic,
        key=f"key{i}",
        value=json.dumps(dict(id=i, value=f"Message {i}")),
        partition=partition or 0,
        timestamp=get_kafka_timestamp(i),
    )

@pytest.fixture(scope="session", autouse=True)
def setup_kafka_for_integration(producer, broker, kafka_admin_client):
    os.environ["KAFKA_BROKERS"] = broker

    topics = list(kafka_admin_client.list_topics().topics.keys())
    topics = [t for t in topics if not t.startswith("__")]
    if topics:
        futures = kafka_admin_client.delete_topics(topics)
        for future in futures.values():
            future.result()

        # future.result() is not enough to wait for deleted topics:(
        time.sleep(1)


@pytest.mark.parametrize("strategy", ["subscribe", "assign"])
@pytest.mark.parametrize("batch_size", ["msg_count_lt_batch", "msg_count_eq_batch", "msg_count_gt_batch"])
def test_many_messages(base_config, kafka_admin_client, transform_config, producer, strategy, batch_size):
    topic = f"test_many_messages_{strategy}_{batch_size}"
    config = build_config(base_config, topic, strategy)
    create_topic(kafka_admin_client, topic, num_partitions=2)
    oracle_target, mapping = setup_mapping(config, transform_config)

    if batch_size == "msg_count_lt_batch":
        messages_count = config["source"]["batch-size"] - 2
    elif batch_size == "msg_count_eq_batch":
        messages_count = config["source"]["batch-size"]
    else:  # "msg_count_gt_batch"
        messages_count = config["source"]["batch-size"] + 2

    for i in range(messages_count):
        produce_default_message(producer, topic, i, partition=i%2)
    producer.flush()

    summary = mapping.run()

    assert summary.event_count == messages_count
    assert summary.data_count == messages_count
    assert summary.error_count == 0
    assert summary.written_to_db_count == messages_count
    assert summary.committed_to_producer_count == (-1 if strategy == "assign" else messages_count)
    assert summary.empty_count == 0
    assert summary.non_empty_count == messages_count

    rows = get_kafka_messages(oracle_target, topic)
    assert len(rows) == messages_count
    for i in range(messages_count):
        assert rows[i].topic == topic
        assert rows[i].key == f"key{i}"
        assert rows[i].offset == i // 2  # starts at 0 for each partition
        assert rows[i].partition == i % 2
        assert rows[i].hash == hashlib.sha256(rows[i].message.encode("utf-8")).hexdigest()
        assert (rows[i].lastet_tid - datetime.now()) < timedelta(seconds=10)
        assert rows[i].object["id"] == i
        assert rows[i].object["value"] == f"Message {i}"


@pytest.mark.parametrize("strategy", ["subscribe", "assign"])
def test_incremental_consumption(base_config, kafka_admin_client, transform_config, producer, strategy):
    """Test that running the mapping multiple times only consumes new messages"""
    topic = f"test_incremental_consumption_{strategy}"
    config = build_config(base_config, topic, strategy)
    create_topic(kafka_admin_client, topic, num_partitions=2)
    oracle_target, mapping = setup_mapping(config, transform_config)

    produce_default_message(producer, topic, 0, partition=0)
    produce_default_message(producer, topic, 1, partition=1)
    producer.flush()

    mapping.run()

    rows = get_kafka_messages(oracle_target, topic)
    ids = [row.object["id"] for row in rows]
    assert ids == [0, 1]

    produce_default_message(producer, topic, 2, partition=0)
    produce_default_message(producer, topic, 3, partition=1)
    producer.flush()

    time.sleep(1)

    mapping.run()

    rows = get_kafka_messages(oracle_target, topic)
    ids = [row.object["id"] for row in rows]
    assert ids == [0, 1, 2, 3]

@pytest.mark.parametrize("strategy", ["subscribe", "assign"])
def test_incremental_consumption_no_new_messages(base_config, kafka_admin_client, transform_config, producer, strategy):
    """Test that running the mapping multiple times without new messages does not duplicate messages"""
    topic = f"test_incremental_consumption_no_new_messages_{strategy}"
    config = build_config(base_config, topic, strategy)
    create_topic(kafka_admin_client, topic, num_partitions=2)
    oracle_target, mapping = setup_mapping(config, transform_config)

    produce_default_message(producer, topic, 0, partition=0)
    produce_default_message(producer, topic, 1, partition=1)
    producer.flush()

    mapping.run()

    rows = get_kafka_messages(oracle_target, topic)
    ids = [row.object["id"] for row in rows]
    assert ids == [0, 1]

    mapping.run()

    rows = get_kafka_messages(oracle_target, topic)
    ids = [row.object["id"] for row in rows]
    assert ids == [0, 1]

def produce_field_test_data(producer, topic):
    for i in range(2):
        producer.produce(
            topic,
            key=f"key{i}",
            value=json.dumps(
                {
                    "id": i,
                    "value": f"Message {i}",
                    "string": "hei",
                    "nested": {"key": "test"},
                    "nested2": None,
                    "nested3": {"key": "test"},
                    "nested4": [{"index": "test"}],
                    "nested5": [
                        {
                            "key1": "test",
                        },
                        {"key2": "test"},
                        {"key2": None},
                    ],
                    "nested6": [{"nested7": [{"key": "val"}]}],
                }
            ),  # NB husk å teste med flere felter her
            partition=i % 2,
            timestamp=get_kafka_timestamp(i),
        )
    producer.flush()


@pytest.mark.parametrize("strategy", ["subscribe", "assign"])
def test_run_assign_flag_field(base_config, kafka_admin_client, transform_config, producer, strategy):
    topic = f"test_run_assign_flag_field_{strategy}"
    config = build_config(base_config, topic, strategy)
    config["source"]["flag-field-config"] = [
        "string",
        "nested",
        "nested2",
        "nested3/key",
        "nested4/index",
        "nested5/key2",
        "nested6/nested7/key",
    ]
    config["source"]["keypath-seperator"] = "/"
    create_topic(kafka_admin_client, topic, num_partitions=2)
    oracle_target, mapping = setup_mapping(config, transform_config)

    produce_field_test_data(producer, topic)

    mapping.run()

    rows = get_kafka_messages(oracle_target, topic)
    obj = rows[0].object

    assert obj["id"] == 0
    assert obj["value"] == "Message 0"
    assert obj["string"] == 1
    assert obj["nested"] == 1
    assert obj["nested2"] == 0
    assert obj["nested3"]["key"] == 1
    assert obj["nested4"][0]["index"] == 1
    assert obj["nested5"][0]["key1"] == "test"
    assert obj["nested5"][1]["key2"] == 1
    assert obj["nested5"][2]["key2"] == 0
    assert obj["nested6"][0]["nested7"][0]["key"] == 1

    assert rows[1].object["id"] == 1

@pytest.mark.parametrize("strategy", ["subscribe", "assign"])
def test_filter_field(base_config, kafka_admin_client, transform_config, producer, strategy):
    topic = f"test_filter_field_{strategy}"
    config = build_config(base_config, topic, strategy)
    config["source"]["message-fields-filter"] = [
        "string",
        "nested",
        "nested2",
        "nested3/key",
        "nested4/index",
        "nested5/key2",
        "nested6/nested7/key",
    ]
    config["source"]["keypath-seperator"] = "/"
    create_topic(kafka_admin_client, topic, num_partitions=2)
    oracle_target, mapping = setup_mapping(config, transform_config)

    produce_field_test_data(producer, topic)

    mapping.run()

    rows = get_kafka_messages(oracle_target, topic)
    obj = rows[0].object

    assert obj["id"] == 0
    assert obj["value"] == "Message 0"
    assert "string" not in obj
    assert "nested" not in obj
    assert "nested2" not in obj
    assert "key" not in obj["nested3"]
    assert "index" not in obj["nested4"][0]
    assert obj["nested5"][0]["key1"] == "test"
    assert "key2" not in obj["nested5"][1]
    assert "key" not in obj["nested6"][0]["nested7"][0]

    assert rows[1].object["id"] == 1

@contextmanager
def mock_poll(source: KafkaSource, poll_fn: Callable, retriable: bool, fatal: bool):
    message_mock = MagicMock()
    error_mock = MagicMock()
    error_mock.retriable.return_value = retriable
    error_mock.fatal.return_value = fatal
    message_mock.error.return_value = error_mock

    call_count = -1
    def poll_wrapper():
        nonlocal call_count
        call_count += 1
        return poll_fn(source, message_mock, error_mock, call_count)

    with patch.object(source, "_poll") as mock_poll:
        mock_poll.side_effect = poll_wrapper
        yield


@pytest.mark.parametrize("strategy", ["subscribe", "assign"])
def test_fatal_error(base_config, kafka_admin_client, transform_config, producer, strategy):
    """Test that a fatal error commits processed messages, but no further messages are processed"""
    topic = f"test_fatal_error_{strategy}"
    config = build_config(base_config, topic, strategy)
    create_topic(kafka_admin_client, topic)
    oracle_target, mapping = setup_mapping(config, transform_config)

    produce_default_message(producer, topic, 0)
    produce_default_message(producer, topic, 1)
    producer.flush()

    def fake_poll(source, message_mock, error_mock, call_counter):
        if call_counter == 1:
            return message_mock
        else:
            return source.consumer.poll(timeout=5.0)

    with mock_poll(mapping.source, fake_poll, retriable=False, fatal=True):
        with pytest.raises(RuntimeError) as exc:
            mapping.run()

    assert isinstance(exc.value.args[1], ProcessSummary)
    summary = exc.value.args[1]
    assert summary.event_count == 2
    assert summary.data_count == 1
    assert summary.error_count == 1
    assert summary.written_to_db_count == 1
    assert summary.empty_count == 0
    assert summary.non_empty_count == 2
    assert summary.committed_to_producer_count == (-1 if strategy == "assign" else 1)

    rows = get_kafka_messages(oracle_target, topic)
    ids = [row.object["id"] for row in rows]
    assert ids == [0]

    # process the rest of it
    summary = mapping.run()
    rows = get_kafka_messages(oracle_target, topic)
    ids = [row.object["id"] for row in rows]
    assert ids == [0, 1]
    assert summary.event_count == (2 if strategy == "assign" else 1)
    assert summary.data_count == (2 if strategy == "assign" else 1)
    assert summary.error_count == 0
    assert summary.written_to_db_count == (2 if strategy == "assign" else 1)  # OracleTarget does deduplication
    assert summary.empty_count == 0
    assert summary.non_empty_count == (2 if strategy == "assign" else 1)
    assert summary.committed_to_producer_count == (-1 if strategy == "assign" else 1)


@pytest.mark.parametrize("strategy", ["subscribe", "assign"])
def test_retriable_error(base_config, kafka_admin_client, transform_config, producer, strategy):
    """Test that a retriable error retries processing the message and continues processing further messages"""
    topic = f"test_retriable_error_{strategy}"
    config = build_config(base_config, topic, strategy)
    create_topic(kafka_admin_client, topic)
    oracle_target, mapping = setup_mapping(config, transform_config)

    produce_default_message(producer, topic, 0)
    produce_default_message(producer, topic, 1)
    producer.flush()

    def fake_poll(source, message_mock, error_mock, call_counter):
        if call_counter == 0:
            return message_mock
        else:
            return source.consumer.poll(timeout=5.0)

    with mock_poll(mapping.source, fake_poll, retriable=True, fatal=False):
        summary = mapping.run()

    assert summary.event_count == 3
    assert summary.data_count == 2
    assert summary.error_count == 1
    assert summary.written_to_db_count == 2
    assert summary.empty_count == 0
    assert summary.non_empty_count == 3
    assert summary.committed_to_producer_count == (-1 if strategy == "assign" else 2)

    rows = get_kafka_messages(oracle_target, topic)
    ids = [row.object["id"] for row in rows]
    assert ids == [0, 1]


def test_assign_poll_timeout(base_config, kafka_admin_client, transform_config, producer):
    """todo: why is there no handling of poll timeout to register an empty event in subscribe mode?"""
    topic = f"test_assign_poll_timeout"
    config = build_config(base_config, topic, "assign")
    create_topic(kafka_admin_client, topic)
    oracle_target, mapping = setup_mapping(config, transform_config)

    produce_default_message(producer, topic, 0)
    producer.flush()

    counter = 0
    def fake_poll():
        nonlocal counter
        if counter == 0:
            counter += 1
            return None
        else:
            return mapping.source.consumer.poll(timeout=5.0)

    with patch.object(mapping.source, "_poll", new=fake_poll):
        summary = mapping.run()

    assert summary.event_count == 1
    assert summary.data_count == 1
    assert summary.error_count == 0
    assert summary.written_to_db_count == 1
    assert summary.empty_count == 1
    assert summary.non_empty_count == 1
    assert summary.committed_to_producer_count == -1

    rows = get_kafka_messages(oracle_target, topic)
    assert len(rows) == 1

