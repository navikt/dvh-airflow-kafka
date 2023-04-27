import os
import yaml
import pytest
import environment
from unittest import mock
import json

from schema_registry.client import SchemaRegistryClient, schema

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
avro_schema_file = os.path.join(__location__, 'test.avsc')


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
        "KAFKA_CA_PATH": "",
        "DATA_INTERVAL_START": "1580261194785",
        "DATA_INTERVAL_END": "1780261194785"
    }

    with open(avro_schema_file, "r") as f:
        test_env["AVRO_MESSAGE_SCHEMA"] = f.read()
    with mock.patch.dict(os.environ, test_env, clear=True):
        yield


@pytest.fixture()
def test_config():
    test_config_file = os.path.join(
        __location__, 'kafka-configs/test-config.yml')
    with open(test_config_file) as stream:
        test_config = yaml.safe_load(stream)
    return test_config


@pytest.fixture()
def test_config_json():
    test_config_file = os.path.join(
        __location__, 'kafka-configs/test-config_json.yml')
    with open(test_config_file) as stream:
        test_config = yaml.safe_load(stream)
    return test_config


@pytest.fixture()
def register_avro():
    avro_file_path = os.path.realpath(
        os.path.join(__location__, "test.avsc")
    )

    with (open(avro_file_path, "r")) as avro_file:
        avro_file_contents = avro_file.read()

    client = SchemaRegistryClient(url=os.environ["KAFKA_SCHEMA_REGISTRY"])
    avro_schema = schema.AvroSchema(avro_file_contents)
    schema_id = client.register("test", avro_schema)
    return schema_id


@pytest.fixture()
def avro_message(register_avro):
    avro_message_file = os.path.join(
        __location__, 'kafka-messages/melding-avro.json')
    with open(avro_message_file) as f:
        avro_message = json.loads(f.read())
    return avro_message
