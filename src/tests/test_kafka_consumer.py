from typing import Any, Dict, Text
import pytest
import os

from unittest import mock
import yaml
from kafka_source import KafkaSource
from fixtures.fixtures import test_config


class LocalKafkaSource(KafkaSource):
    def __init__(self, config: Dict[Text, Any]) -> None:
        super().__init__(config)

    def set_consumer(self):
        # kafka.KafkaConsumer

        print(os.environ["KAFKA_BROKERS"].split(","))
        self.consumer = KafkaSource.connection_class(
            self.config["topic"],
            auto_offset_reset="earliest",
            security_protocol="PLAINTEXT",
            enable_auto_commit=False,
            bootstrap_servers=os.environ["KAFKA_BROKERS"].split(","),
            key_deserializer=KafkaSource._key_deserializer,
            value_deserializer=self.value_deserializer,
        )


@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    test_env = {
        "NAIS_APP_NAME": "pytest",
        # "KAFKA_SCHEMA_REGISTRY": "http://127.0.0.1:8085",
        "KAFKA_SCHEMA_REGISTRY_USER": "",
        "KAFKA_SCHEMA_REGISTRY_PASSWORD": "",
        'KAFKA_BROKERS': "kafka:9092",
        "KAFKA_CERTIFICATE_PATH": "",
        "KAFKA_PRIVATE_KEY_PATH": "",
        "KAFKA_CA_PATH": ""
    }
    with mock.patch.dict(os.environ, test_env, clear=True):
        yield


@pytest.fixture()
def test_config():
    config_file_path = os.path.realpath(
        os.path.join(os.getcwd(),
                     os.path.dirname(__file__),
                     "config-test.yml")
    )
    with open(config_file_path,  mode="r", encoding="utf-8") as f:
        yield yaml.safe_load(stream=f)


def build_test_source(source_config: Dict):
    return LocalKafkaSource(source_config)


@pytest.fixture(autouse=True)
def set_kafka_consumer(monkeypatch):
    monkeypatch.setattr('dwh_consumer.mapping.build_source', build_test_source)


@pytest.mark.integration
def test_kafka_should_write_end_offset(test_config):
    assert False
    # mapping = Mapping(test_config)
    # mapping.run(once=True)

    # end_offsets = mapping.source.end_offsets()
    # assert mapping.target.buffer[0]["kafka_end_offset"] == end_offsets[0]
