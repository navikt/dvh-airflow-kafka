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
from fixtures.fixtures import register_avro, test_config, avro_message, mock_settings_env_vars


@pytest.mark.integration
def test_consume_avro_message(test_config, avro_message):
    target = KafkaTarget(config=test_config["target"])
    target.write_batch([avro_message])
    source = KafkaSource(config=test_config["source"])
    msgs = [msg for msg in source.read_polled_batches()]
    last_message_produced = avro_message
    last_message_consumed = json.loads(msgs[-1][-1]["kafka_message"])
    assert last_message_consumed == last_message_produced


@pytest.fixture()
def mock_connection_class():
    stash = KafkaSource.connection_class
    KafkaSource.connection_class = mock.MagicMock()
    yield KafkaSource
    KafkaSource.connection_class = stash
