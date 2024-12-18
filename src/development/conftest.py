import os
import pytest
import json
import yaml
from datetime import datetime, timedelta
import uuid

from testcontainers.kafka import KafkaContainer
from testcontainers.oracle import OracleDbContainer

from confluent_kafka import Consumer, Producer
from ..oracle_target import OracleTarget
from ..kafka_source import KafkaSource

kafka = KafkaContainer()
oracle = OracleDbContainer()
n_kafka_messages = 20
now = datetime(2024, 12, 18, 11, 11, 11)
os.environ["ENVIRONMENT"] = "LOCAL"


@pytest.fixture(scope="session")
def topic_name():
    return "test_topic"


@pytest.fixture(scope="session")
def table_name():
    return "RAA_DATA_STROM"


@pytest.fixture(scope="session")
def broker():
    # Start Kafka container
    kafka.start()
    # Get the Kafka broker URL
    broker = kafka.get_bootstrap_server()
    return broker


@pytest.fixture(scope="module")
def consumer_config(broker):
    return {
        "bootstrap.servers": broker,
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 12000,
        "session.timeout.ms": 10000,
    }


@pytest.fixture(scope="session")
def producer_config(broker):
    return {
        "bootstrap.servers": broker,
    }


@pytest.fixture(scope="module")
def producer(producer_config):
    return Producer(producer_config)


@pytest.fixture(scope="function")
def consumer(consumer_config, topic_name):
    # Create a Kafka consumer

    consumer = Consumer(consumer_config)
    consumer.subscribe([topic_name])
    return consumer


@pytest.fixture(autouse=True, scope="module")
def setUpKafka(producer, topic_name, broker):
    os.environ["KAFKA_BROKERS"] = broker
    for i in range(n_kafka_messages):
        producer.produce(
            topic_name,
            key=f"key{i}",
            value=json.dumps({"id": i, "value": f"Message {i}"}),
            partition=0,
            timestamp=int(datetime.timestamp(now - timedelta(days=(n_kafka_messages - i - 1)))),
        )
    producer.flush()


@pytest.fixture(scope="session")
def base_config(topic_name, table_name):
    yaml_config = f"""
    source:
      type: kafka
      batch-size: 10
      batch-interval: 5
      topic: {topic_name}
      group-id: group1
      schema: json
      poll-timeout: 10
      strategy: subscribe
    target:  
      type: oracle
      table: {table_name}
      skip-duplicates-with: 
        - kafka_hash
    """
    return yaml.safe_load(stream=yaml_config)


@pytest.fixture(scope="session")
def kafka_source(base_config):
    return KafkaSource(base_config["source"])


@pytest.fixture(scope="session")
def oracle_target(base_config):
    return OracleTarget(base_config["target"])


@pytest.fixture(scope="session")
def kafka_source_assign(topic_name):
    yaml_config = f"""
    source:
      type: kafka
      batch-size: 10
      batch-interval: 5
      topic: {topic_name}
      group-id: group1
      schema: json
      poll-timeout: 10
      strategy: assign
    """
    return KafkaSource(yaml.safe_load(stream=yaml_config)["source"])


@pytest.fixture(scope="session")
def transform_config():
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


@pytest.fixture(autouse=True, scope="module")
def setup_oracle(table_name, transform_config):
    oracle.start()
    url = oracle.get_connection_url()
    dsn = url.split("@")[1]
    password = url.split("@")[0].split(":")[-1]
    os.environ["DB_USER"] = "SYSTEM"
    os.environ["DB_PASSWORD"] = password
    os.environ["DB_DSN"] = dsn

    columns = [f"{obj["dst"]} {obj["datatype"]}" for obj in transform_config]
    sql = f"""create table SYSTEM.{table_name} ({",".join(columns)}) """

    with OracleTarget._oracle_connection() as con:
        with con.cursor() as cur:
            cur.execute(sql)
        con.commit()
