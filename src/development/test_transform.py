import pytest

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
