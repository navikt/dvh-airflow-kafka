import pytest
import json

from confluent_kafka import Consumer
from confluent_kafka.admin import NewTopic

from ..kafka_source import KafkaSource

TOPIC_NAME = "test-topic-kafka-source"


@pytest.fixture(autouse=True)
def setUpKafka(producer, kafka_admin_client):
    kafka_admin_client.create_topics([NewTopic(TOPIC_NAME, 2)])
    for i in range(4):
        producer.produce(
            TOPIC_NAME,
            key=f"key{i}",
            value=json.dumps({"id": i, "value": f"Message {i}", "enum": "INTERESTING"}),
            partition=i % 2,
        )
    for i in range(4, 8):
        producer.produce(
            TOPIC_NAME,
            key=f"key{i}",
            value=json.dumps({"id": i, "value": f"Message {i}", "enum": "NOT_RELEVANT"}),
            partition=i % 2,
        )
    producer.flush()


@pytest.fixture
def config1(base_config):
    config = base_config
    config["source"]["topic"] = TOPIC_NAME
    config["source"]["strategy"] = "subscribe"
    config["source"]["group-id"] = "test_kafka_source"
    config["source"]["message-filters"] = [{"key": "enum", "allowed_value": "INTERESTING"}]
    return config


def test_collect_message(consumer, config1):
    consumer.subscribe([TOPIC_NAME])
    messages = [consumer.poll() for _ in range(8)]
    # consumer.commit()
    consumer.close()

    source = KafkaSource(config1["source"])

    filtered_messages = [source.collect_message(m) for m in messages]
    assert filtered_messages[0]["kafka_message"] is not None
    assert filtered_messages[1]["kafka_message"] is not None
    assert filtered_messages[4]["kafka_message"] is not None
    assert filtered_messages[5]["kafka_message"] is not None
    assert filtered_messages[2]["kafka_message"] == None
    assert filtered_messages[3]["kafka_message"] == None
    assert filtered_messages[6]["kafka_message"] == None
    assert filtered_messages[7]["kafka_message"] == None


@pytest.fixture
def config_no_message_filter(base_config):
    config = base_config
    config["source"]["topic"] = TOPIC_NAME
    config["source"]["strategy"] = "subscribe"
    config["source"]["group-id"] = "test_kafka_source"
    return config


def test_collect_message(consumer, config_no_message_filter):
    consumer.subscribe([TOPIC_NAME])
    messages = [consumer.poll() for _ in range(8)]
    # consumer.commit()
    consumer.close()

    source = KafkaSource(config_no_message_filter["source"])

    filtered_messages = [source.collect_message(m) for m in messages]
    assert filtered_messages[0]["kafka_message"] is not None
    assert filtered_messages[1]["kafka_message"] is not None
    assert filtered_messages[4]["kafka_message"] is not None
    assert filtered_messages[5]["kafka_message"] is not None
    assert filtered_messages[2]["kafka_message"] is not None
    assert filtered_messages[3]["kafka_message"] is not None
    assert filtered_messages[6]["kafka_message"] is not None
    assert filtered_messages[7]["kafka_message"] is not None
