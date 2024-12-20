import pytest
import os
from datetime import datetime, timedelta
import json

from confluent_kafka.admin import NewTopic

from ..transform import Transform
from ..mapping import Mapping
from ..kafka_source import KafkaSource
from ..oracle_target import OracleTarget

import pyinstrument

TABLE_NAME = "RAA_DATA_STROM"
TOPIC_NAME = "integration-test-topic"
n_kafka_messages = 10000
now = datetime(2024, 12, 18, 11, 11, 11)


@pytest.fixture
def subscribe_config(base_config):
    config = base_config
    config["source"]["topic"] = TOPIC_NAME
    config["source"]["strategy"] = "subscribe"
    config["source"]["group-id"] = "integration-test-group-id-1"

    config["target"]["table"] = TABLE_NAME
    return config


@pytest.fixture
def assign_config(base_config):
    config = base_config
    config["source"]["topic"] = TOPIC_NAME
    config["source"]["strategy"] = "assign"
    config["source"]["group-id"] = "integration-test-group-id-2"
    config["target"]["table"] = TABLE_NAME
    return config


@pytest.fixture(autouse=True)
def setup_kafka_for_integration(producer, broker, kafka_admin_client, consumer):
    os.environ["KAFKA_BROKERS"] = broker
    kafka_admin_client.create_topics([NewTopic(TOPIC_NAME, 2)])
    for i in range(n_kafka_messages):
        producer.produce(
            TOPIC_NAME,
            key=f"key{i}",
            value=json.dumps({"id": i, "value": f"Message {i}"}),
            partition=i % 2,
            timestamp=int(datetime.timestamp(now - timedelta(days=(n_kafka_messages - i - 1)))),
        )
    producer.flush()

    consumer.subscribe([TOPIC_NAME])


@pyinstrument.profile()
def test_run_subscribe(subscribe_config, transform_config):
    transform = Transform(transform_config)
    kafka_source = KafkaSource(subscribe_config["source"])
    oracle_target = OracleTarget(subscribe_config["target"])
    mapping = Mapping(kafka_source, oracle_target, transform)

    mapping.run()

    with oracle_target._oracle_connection() as con:
        with con.cursor() as cur:
            table_name = oracle_target.config.table
            cur.execute(f"select kafka_key, kafka_topic from {table_name}")
            r = cur.fetchone()
    assert r[0] == "key1"
    assert r[1] == TOPIC_NAME

    with oracle_target._oracle_connection() as con:
        with con.cursor() as cur:
            table_name = oracle_target.config.table
            cur.execute(f"select count(*) from {table_name}")
            r = cur.fetchone()
    assert r[0] == 10000


@pyinstrument.profile()
def test_run_assign(assign_config, transform_config):
    os.environ["DATA_INTERVAL_START"] = str(
        int(datetime.timestamp(now - timedelta(days=(n_kafka_messages))))
    )
    os.environ["DATA_INTERVAL_END"] = str(
        int(
            datetime.timestamp(now + timedelta(days=(n_kafka_messages - n_kafka_messages // 2 - 1)))
        )
    )
    kafka_source = KafkaSource(assign_config["source"])
    oracle_target = OracleTarget(assign_config["target"])
    transform = Transform(transform_config)
    mapping = Mapping(kafka_source, oracle_target, transform)

    mapping.run()

    with oracle_target._oracle_connection() as con:
        with con.cursor() as cur:
            table_name = oracle_target.config.table
            cur.execute(f"select kafka_key, kafka_topic from {table_name}")
            r = cur.fetchone()
    assert r[1] == TOPIC_NAME

    with oracle_target._oracle_connection() as con:
        with con.cursor() as cur:
            table_name = oracle_target.config.table
            cur.execute(f"select count(*) from {table_name}")
            r = cur.fetchone()
    assert r[0] == 10000
