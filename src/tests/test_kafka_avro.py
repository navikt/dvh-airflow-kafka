import pytest
import os
import yaml
import json
import environment

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from kafka_source import KafkaSource
from kafka_target import KafkaTarget
from unittest import mock

from schema_registry.client import SchemaRegistryClient, schema

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
avro_schema_file = os.path.join(__location__, 'test.avsc')




"""
producer = KafkaProducer(
        security_protocol="PLAINTEXT",
        bootstrap_servers="localhost:9092",
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
"""


@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    environment.isNotLocal = False
    test_env = {
        "KAFKA_SCHEMA_REGISTRY": "http://127.0.0.1:8085",
        "KAFKA_SCHEMA_REGISTRY_USER": "",
        "KAFKA_SCHEMA_REGISTRY_PASSWORD": "",
        'KAFKA_BROKERS': "127.0.0.1:9092",
        "KAFKA_CERTIFICATE_PATH": "",
        "KAFKA_PRIVATE_KEY_PATH": "",
        "KAFKA_CA_PATH": ""
    }

    with open(avro_schema_file, "r") as avro_stream:
        test_env["AVRO_MESSAGE_SCHEMA"] = avro_stream.read()
    with mock.patch.dict(os.environ, test_env, clear=True):
        yield


@pytest.fixture()
def test_config():
    test_config_file = os.path.join(__location__, 'test-config.yml')
    with open(test_config_file) as stream:
        test_config = yaml.safe_load(stream)
    return test_config


@pytest.fixture()
def avro_message():
    avro_message_file = os.path.join(__location__, 'melding-avro.json')
    with open(avro_message_file) as stream:
        avro_message = json.loads(stream.read())
    return avro_message


@pytest.fixture()
def register_avro():
    client = SchemaRegistryClient(url=os.environ["KAFKA_SCHEMA_REGISTRY"])
    avro_schema = schema.AvroSchema(os.environ['AVRO_MESSAGE_SCHEMA'])
    schema_id = client.register("test", avro_schema)
    return schema_id


@pytest.fixture()
def produce_avro_message(test_config, avro_message):
    target = KafkaTarget(config=test_config['target'])
    target.write_batch([avro_message])


@pytest.fixture()
def avro_produce(register_avro, produce_avro_message):
    pass


@pytest.mark.integration
def test_consume_avro_message(avro_produce, test_config):
    # source = KafkaSource(test_config['source'])
    assert False


@pytest.fixture()
def mock_connection_class():
    stash = KafkaSource.connection_class
    KafkaSource.connection_class = mock.MagicMock()
    yield KafkaSource
    KafkaSource.connection_class = stash


@pytest.mark.integration
def test_consume(mock_connection_class, write_to_topic):

    
    # kafka.KafkaConsumer
    test_config = {
        "topic": "integration-test",
        "schema": "json",
        "batch-interval": 5,
        "batch-size": 1
    }


    consumer: KafkaConsumer = KafkaConsumer(
        test_config["topic"],
        security_protocol="PLAINTEXT",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        bootstrap_servers=["kafka:9092"],
        key_deserializer=ku.key_deserializer,
        value_deserializer=ku.json_deserializer,
        consumer_timeout_ms=1000,
    )
    
    kafka_source = KafkaSource(config=test_config)
    
    kafka_source.consumer = consumer
    
    consumer.topics()
    end_offsets = consumer.end_offsets(consumer.assignment())
    end_offsets = {tp.partition: offset for tp, offset in end_offsets.items()}

    for value in end_offsets.values():
        assert value > 0, "Should be some messages on the topic."

    for batch in kafka_source.read_batches():
        print(batch)
        break

    consumer.close()
    