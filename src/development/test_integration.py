import time
from dataclasses import dataclass

import pytest
import os
from datetime import datetime, timedelta
import json

from confluent_kafka.admin import NewTopic

from src.mapping import Mapping
from src.oracle_target import OracleTarget

from ..transform import Transform
from ..mapping import Mapping
from ..kafka_source import KafkaSource
from ..oracle_target import OracleTarget

import pyinstrument

TABLE_NAME = "RAA_DATA_STROM"
NOW = datetime(2024, 12, 18, 11, 11, 11)
MAX_NUM_KAFKA_MESSAGES = 1000


def build_config(base_config, topic_name: str, strategy: str) -> dict:
    base_config["source"]["topic"] = topic_name
    base_config["source"]["strategy"] = strategy
    base_config["source"]["group-id"] = topic_name
    base_config["target"]["table"] = TABLE_NAME
    return base_config


def setup_mapping( assign_config, transform_config) -> tuple[OracleTarget, Mapping]:
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
    topic: str
    message: str
    object: dict

def get_kafka_messages(oracle_target, topic) -> list[Row]:
    with oracle_target.oracle_connection() as con:
        with con.cursor() as cur:
            table_name = oracle_target.config.table
            cur.execute(f"select kafka_key, kafka_topic, kafka_message from {table_name} "
                        f"where kafka_topic = :topic order by kafka_timestamp",
                        topic=topic)
            rows = [Row(r[0], r[1], r[2].read(), json.loads(r[2].read())) for r in cur.fetchall()]
            return rows


def create_topic(kafka_admin_client, topic_name: str, num_partitions: int = 1):
    futures = kafka_admin_client.create_topics(
        [NewTopic(topic=topic_name, num_partitions=num_partitions)]
    )
    futures[topic_name].result()

def get_kafka_timestamp(message_offset: int) -> int:
    return int(datetime.timestamp(NOW - timedelta(days=(MAX_NUM_KAFKA_MESSAGES - message_offset - 1))))

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
def test_many_messages_produced(base_config, kafka_admin_client, transform_config, producer, strategy):
    topic = f"test_many_messages_produced_{strategy}"
    config = build_config(base_config, topic, strategy)
    create_topic(kafka_admin_client, topic, num_partitions=2)
    oracle_target, mapping = setup_mapping(config, transform_config)

    for i in range(100):
        producer.produce(
            topic,
            key=f"key{i}",
            value=json.dumps(dict(id=i, value=f"Message {i}")),
            partition=i % 2,
            timestamp=get_kafka_timestamp(i),
        )
    producer.flush()

    mapping.run()

    rows = get_kafka_messages(oracle_target, topic)
    assert len(rows) == 100
    assert rows[0].topic == topic
    assert rows[0].key == "key0"
    assert rows[0].object["id"] == 0
    assert rows[0].object["value"] == "Message 0"

    assert rows[1].object["id"] == 1


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
                    "nested4": {"index": "test"},
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

    mapping.run()

    rows = get_kafka_messages(oracle_target, topic)
    obj = rows[0].object

    assert obj["nested"] == 1
    assert obj["nested2"] == 0
    assert obj["nested5"][0]["key1"] == "test"
    assert obj["nested5"][1]["key2"] == 1
    assert obj["nested5"][2]["key2"] == 0
    assert obj["nested6"][0]["nested7"][0]["key"] == 1

    assert rows[1].object["id"] == 1
