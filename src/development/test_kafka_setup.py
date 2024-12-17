import pytest
import json


@pytest.fixture(scope="module", autouse=True)
def setUp(producer, topic_name):
    n_messages = 20
    for i in range(n_messages):
        producer.produce(
            topic_name,
            key=f"key{i}",
            value=json.dumps({"id": i, "value": f"Message {i}"}),
            partition=0,
        )
    producer.flush()


def test_consumer(consumer):
    m1 = consumer.poll()
    m2 = consumer.poll()
    assert json.loads(m2.value())["id"] == 1

    consumer.commit()


def test_consumer_continue_at_offset(consumer):
    m3 = consumer.poll()

    assert json.loads(m3.value())["id"] == 2


def test_consumer_continue_at_same_offset_after_no_commit(consumer):
    m3 = consumer.poll()

    assert json.loads(m3.value())["id"] == 2
