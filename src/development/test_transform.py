import pytest
from datetime import datetime, timezone

from ..transform import Transform


# TODO $$$BATCH_TIME i airflow -> envvariable -> dict blir $$BATCH_TIME
@pytest.fixture(scope="session")
def config():
    return [
        {"src": "kafka_key", "dst": "kafka_key", "datatype": "VARCHAR2(255)"},
        {"src": "kafka_offset", "dst": "kafka_offset", "datatype": "NUMBER"},
        {"src": "kafka_partition", "dst": "kafka_partition", "datatype": "NUMBER"},
        {
            "src": "kafka_timestamp",
            "dst": "kafka_timestamp",
            "datatype": "TIMESTAMP",
            "fun": "int-unix-ms -> datetime-no",
        },
        {"src": "kafka_topic", "dst": "kafka_topic", "datatype": "VARCHAR2(255)"},
        {"src": "kafka_hash", "dst": "kafka_hash", "datatype": "VARCHAR2(255)"},
        {"src": "kafka_message", "dst": "kafka_message", "datatype": "CLOB"},
        {"src": "$TESTERSEN", "dst": "KILDESYSTEM", "datatype": "VARCHAR2(255)"},
        {"src": "$$BATCH_TIME", "dst": "lastet_tid", "datatype": "TIMESTAMP"},
    ]


def test_transform(config):

    trans = Transform(config)


def test_timestamp():
    now = datetime.now()
    now = datetime.timestamp(now)
    y1 = datetime.utcfromtimestamp(now)
    y2 = datetime.fromtimestamp(now, timezone.utc)
    assert y1.year == y2.year
    assert y1.month == y2.month
    assert y1.day == y2.day
    assert y1.minute == y2.minute
    assert y1.second == y2.second
    assert y1.microsecond == y2.microsecond
