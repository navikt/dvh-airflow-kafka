import pytest
import json


@pytest.mark.run("first")
def test_consumer(consumer):
    m1 = consumer.poll()
    assert m1.timestamp()[1] == 1732875071
    m2 = consumer.poll()
    assert json.loads(m2.value())["id"] == 1

    consumer.commit()
    consumer.close()


@pytest.mark.run("second")
def test_consumer_continue_at_offset(consumer):
    m3 = consumer.poll()
    consumer.close()
    assert json.loads(m3.value())["id"] == 2


@pytest.mark.run("last")
def test_consumer_continue_at_same_offset_after_no_commit(consumer):
    m3 = consumer.poll()
    consumer.close()
    assert json.loads(m3.value())["id"] == 2
