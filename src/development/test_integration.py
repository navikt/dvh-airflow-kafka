import time

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
TOPIC_NAME_MORE_DATA = "integration-test-topic-more-data"
n_kafka_messages = 100
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


@pytest.fixture(scope="session", autouse=True)
def setup_kafka_for_integration(producer, broker, kafka_admin_client):
    os.environ["KAFKA_BROKERS"] = broker

    topics = list(kafka_admin_client.list_topics().topics.keys())
    topics = [t for t in topics if not t.startswith("__")]
    if topics:
        futures = kafka_admin_client.delete_topics(topics)
        for future in futures.values():
            future.result()

        # future.result() is not enough :(
        time.sleep(1)

    futures = kafka_admin_client.create_topics(
        [
            NewTopic(topic=TOPIC_NAME, num_partitions=2),
            NewTopic(TOPIC_NAME_MORE_DATA, 2),
        ]
    )
    futures[TOPIC_NAME].result()
    futures[TOPIC_NAME_MORE_DATA].result()

    for i in range(n_kafka_messages):
        producer.produce(
            TOPIC_NAME,
            key=f"key{i}",
            value=json.dumps(
                {
                    "id": i,
                    "value": f"Message {i}",
                }
            ),
            partition=i % 2,
            timestamp=int(datetime.timestamp(now - timedelta(days=(n_kafka_messages - i - 1)))),
        )
    producer.flush()

    # part 2
    for i in range(2):
        producer.produce(
            TOPIC_NAME_MORE_DATA,
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
            timestamp=int(datetime.timestamp(now - timedelta(days=(n_kafka_messages - i - 1)))),
        )
    producer.flush()


class TestSubscribe:

    @pyinstrument.profile()
    def test_run_subscribe(self, subscribe_config, transform_config):
        transform = Transform(transform_config)
        kafka_source = KafkaSource(subscribe_config["source"])
        oracle_target = OracleTarget(subscribe_config["target"])
        mapping = Mapping(kafka_source, oracle_target, transform)

        mapping.run()

        with oracle_target._oracle_connection() as con:
            with con.cursor() as cur:
                table_name = oracle_target.config.table
                cur.execute(f"select kafka_key, kafka_topic, kafka_message from {table_name}")
                r = cur.fetchone()
        assert r[1] == TOPIC_NAME

        with oracle_target._oracle_connection() as con:
            with con.cursor() as cur:
                table_name = oracle_target.config.table
                cur.execute(f"select count(*) from {table_name}")
                r = cur.fetchone()
        assert r[0] == n_kafka_messages


class TestAssign:
    @pyinstrument.profile()
    def test_run_assign(self, assign_config, transform_config):
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
                cur.execute(f"select kafka_key, kafka_topic, kafka_message from {table_name}")
                r = cur.fetchone()
                print(r[2])
        assert r[1] == TOPIC_NAME

        with oracle_target._oracle_connection() as con:
            with con.cursor() as cur:
                table_name = oracle_target.config.table
                cur.execute(f"select count(*) from {table_name}")
                r = cur.fetchone()
        assert r[0] == n_kafka_messages


    @pytest.fixture
    def assign_config_flag_field(self, assign_config):
        config = assign_config
        config["source"]["topic"] = TOPIC_NAME_MORE_DATA
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
        return config


    @pyinstrument.profile()
    def test_run_assign_flag_field(self, assign_config_flag_field, transform_config):
        assign_config = assign_config_flag_field
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
                cur.execute(f"select kafka_key, kafka_topic, kafka_message from {table_name}")
                r = cur.fetchone()
                obj = json.load(r[2])

        assert obj["nested"] == 1
        assert obj["nested2"] == 0
        assert obj["nested5"][0]["key1"] == "test"
        assert obj["nested5"][1]["key2"] == 1
        assert obj["nested5"][2]["key2"] == 0
        assert obj["nested6"][0]["nested7"][0]["key"] == 1
