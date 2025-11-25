import os
from time import sleep

import docker
import pytest
import json
import yaml
from datetime import datetime, timedelta
import uuid

from testcontainers.core import testcontainers_config
from testcontainers.kafka import KafkaContainer
from testcontainers.oracle import OracleDbContainer
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from .utils.container import get_or_start_container, test_oracle_connection
from ..oracle_target import OracleTarget
from ..kafka_source import KafkaSource

os.environ["ENVIRONMENT"] = "LOCAL"

# Don't start the supervisor container, because it needs to be privileged. The worst
# that can happen is that the container will continue running after a crash. This is only an issue
# when running in a CI environment, and there is a maximum runtime for the tests anyway.
testcontainers_config.ryuk_disabled = True


@pytest.fixture(scope="session", autouse=True)
def start_broker():
    container_name = "testcontainer-dvh-airflow-kafka-broker"
    kafka = KafkaContainer(name=container_name)
    kafka.with_bind_ports(9093, ("127.0.0.1", 9093))
    with get_or_start_container(container_name, kafka):
        yield kafka


@pytest.fixture(scope="session")
def broker(start_broker):
    broker = "127.0.0.1:9093"
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
def setup_oracle():
    container_name = "testcontainer-dvh-airflow-kafka-oracle-db"
    oracle = OracleDbContainer(oracle_password="test", name=container_name, dbname="FREEPDB1")
    oracle.with_bind_ports(1521, ("127.0.0.1", 1521))
    with get_or_start_container(container_name, oracle):

        dsn = "127.0.0.1:1521/FREEPDB1"
        os.environ["DB_USER"] = "SYSTEM"
        os.environ["DB_PASSWORD"] = "test"
        os.environ["DB_DSN"] = dsn

        waited = 0
        while waited < 20:
            if test_oracle_connection("127.0.0.1", 1521, "FREEPDB1", "system", "test"):
                break
            waited += 1
            sleep(1)
        else:
            raise RuntimeError(
                f"Could not connect to Oracle test container '{container_name}' after waiting {waited}s. "
                f"Try restarting the test, or deleting the container with `docker container rm -f {container_name}`.")

        yield


def create_table(table_name, columns):

    sql = f"""create table SYSTEM.{table_name} ({",".join(columns)}) """

    with OracleTarget._oracle_connection() as con:
        with con.cursor() as cur:
            cur.execute(sql)
        con.commit()


def drop_table(table_name):
    sql = f"""drop table SYSTEM.{table_name} """

    with OracleTarget._oracle_connection() as con:
        with con.cursor() as cur:
            cur.execute(sql)
        con.commit()


def table_insert(table_name, data):

    columns = ", ".join(data[0].keys())
    values = ", ".join([f":{key}" for key in data[0].keys()])
    sql = f"INSERT INTO SYSTEM.{table_name} ({columns}) VALUES ({values})"
    with OracleTarget._oracle_connection() as con:
        with con.cursor() as cur:
            cur.executemany(sql, data)
        con.commit()


@pytest.fixture(autouse=True, scope="function")
def setup_oracle_fixtures(table_name, transform_config):

    columns = [f"{obj["dst"]} {obj["datatype"]}" for obj in transform_config]
    create_table(table_name=table_name, columns=columns)

    yield

    drop_table(table_name=table_name)
