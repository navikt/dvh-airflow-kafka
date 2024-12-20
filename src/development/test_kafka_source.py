import pytest
import os
import json
import uuid

from confluent_kafka import Consumer
from confluent_kafka.admin import NewTopic

topic = "kafka_source"


@pytest.fixture
def consumer_2(broker):
    return Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "testersen-id",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 12000,
            "session.timeout.ms": 10000,
            "enable.partition.eof": True,
        }
    )


@pytest.fixture(autouse=True)
def setup_kafka_topic(kafka_admin_client, producer, consumer_2):

    res = kafka_admin_client.create_topics([NewTopic(topic, 2)])
    for i in range(4):
        producer.produce(
            topic,
            key=f"key{i}",
            value=json.dumps({"id": i, "value": f"Message {i}"}),
            partition=i % 2,
        )
    producer.flush()


def test_2_partitions(consumer_2):
    consumer_2.subscribe([topic])
    m = consumer_2.consume(6, 5)
    consumer_2.commit()
    consumer_2.close()


def test_2_partitions2(consumer_2):
    consumer_2.subscribe([topic])
    ms = []
    for i in range(4):
        ms.append(consumer_2.poll())
    assert len(consumer_2.list_topics().topics[topic].partitions.keys()) == 2
    consumer_2.commit()
    consumer_2.close()
