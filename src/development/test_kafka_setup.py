import pytest
import os
import json

from confluent_kafka.admin import NewTopic

TOPIC_NAME = "kafka-test-topic"


@pytest.fixture(autouse=True)
def setUpKafka(producer, kafka_admin_client):
    futures = kafka_admin_client.create_topics([NewTopic(TOPIC_NAME, 2)])
    futures[TOPIC_NAME].result()
    for i in range(4):
        producer.produce(
            TOPIC_NAME,
            key=f"key{i}",
            value=json.dumps({"id": i, "value": f"Message {i}"}),
            partition=i % 2,
        )
    producer.flush()


def test_consumer(consumer):
    consumer.subscribe([TOPIC_NAME])
    m1 = consumer.poll()
    consumer.commit()
    consumer.close()

    assert json.loads(m1.value())["id"] in [0, 1]
