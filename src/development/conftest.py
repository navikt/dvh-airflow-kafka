import os
import pytest
import json
import yaml
from datetime import datetime, timedelta
import uuid

from testcontainers.kafka import KafkaContainer
from testcontainers.oracle import OracleDbContainer
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from ..oracle_target import OracleTarget
from ..kafka_source import KafkaSource

kafka = KafkaContainer()
oracle = OracleDbContainer()
os.environ["ENVIRONMENT"] = "LOCAL"


@pytest.fixture(scope="session", autouse=True)
def start_broker(request):
    # Start Kafka container
    kafka.start()

    def remove_container():
        kafka.stop()

    request.addfinalizer(remove_container)


@pytest.fixture(scope="session")
def broker():
    broker = kafka.get_bootstrap_server()
    return broker


@pytest.fixture(scope="module")
def consumer_config(broker):
    return {
        "bootstrap.servers": broker,
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    }


@pytest.fixture(scope="session")
def producer_config(broker):
    return {
        "bootstrap.servers": broker,
    }


@pytest.fixture(scope="session")
def kafka_admin_client(producer_config):
    return AdminClient(producer_config)


@pytest.fixture(scope="session")
def producer(producer_config):
    return Producer(producer_config)


@pytest.fixture(scope="session")
def table_name():
    return "RAA_DATA_STROM"


@pytest.fixture(scope="function")
def consumer(consumer_config):
    # Create a Kafka consumer
    consumer = Consumer(consumer_config)
    return consumer


@pytest.fixture
def base_config():
    yaml_config = f"""
    source:
      type: kafka
      batch-size: 1000
      schema: json
      poll-timeout: 6
    target:  
      type: oracle
      skip-duplicates-with: 
        - kafka_hash
    """
    return yaml.safe_load(stream=yaml_config)


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


@pytest.fixture(autouse=True, scope="session")
def setup_oracle(request, table_name, transform_config):
    oracle.start()

    def remove_container():
        oracle.stop()

    request.addfinalizer(remove_container)

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


@pytest.fixture(autouse=True, scope="module")
def setup_oracle_fixtures(table_name, transform_config):

    sql = f"""drop table SYSTEM.{table_name} """

    with OracleTarget._oracle_connection() as con:
        with con.cursor() as cur:
            cur.execute(sql)
        con.commit()

    columns = [f"{obj["dst"]} {obj["datatype"]}" for obj in transform_config]
    sql = f"""create table SYSTEM.{table_name} ({",".join(columns)}) """

    with OracleTarget._oracle_connection() as con:
        with con.cursor() as cur:
            cur.execute(sql)
        con.commit()
