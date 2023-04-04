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


@pytest.mark.integration
def test_consume_avro_message(test_config, avro_message):
    target = KafkaTarget(config=test_config['target'])
    target.write_batch([avro_message])
    source = KafkaSource(config=test_config['source'])
    msgs = [msg for msg in source.read_batches()]
    last_message_produced = avro_message
    last_message_consumed = json.loads(msgs[-1][-1]['kafka_message'])
    assert last_message_consumed == last_message_produced


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
    