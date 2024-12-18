import pytest
import json


def test_consumer(consumer):
    m1 = consumer.poll()
    m2 = consumer.poll()
    consumer.commit()
    consumer.close()

    assert json.loads(m2.value())["id"] == 1


def test_consumer_continue_at_offset(consumer):
    m3 = consumer.poll()
    consumer.close()
    assert json.loads(m3.value())["id"] == 2


def test_consumer_continue_at_same_offset_after_no_commit(consumer):
    m3 = consumer.poll()
    consumer.close()
    assert json.loads(m3.value())["id"] == 2
