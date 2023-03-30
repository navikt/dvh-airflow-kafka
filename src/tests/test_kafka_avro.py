import pytest
import os
# import dwh_consumer.utils.kafka_utils as ku

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from kafka_source import KafkaSource
# from dwh_consumer.targets import KafkaTarget
from unittest import mock

from schema_registry.client import SchemaRegistryClient, schema



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
    test_env = {
        "KAFKA_SCHEMA_REGISTRY": "http://127.0.0.1:8085",
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
def register_avro():
    avro_file_path = os.path.realpath(
        os.path.join(os.getcwd(),
        os.path.dirname(__file__),
        "test.avcs")
    )
    
    with(open(avro_file_path, "r")) as avro_file:
        avro_file_contents = avro_file.read()

    client = SchemaRegistryClient(url=os.environ["KAFKA_SCHEMA_REGISTRY"])
    avro_schema = schema.AvroSchema(avro_file_contents)
    schema_id = client.register("test", avro_schema)    
    return schema_id


@pytest.fixture()
def register_avro_topic():
    pass


@pytest.mark.integration
def test_produce(register_avro):
    #target = KafkaTarget()
    assert True

@pytest.fixture
def write_to_topic():
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=ku.json_serializer
    )
    message = {
        "id": 1,
        "verdi": "Haha",
    }
    future = producer.send(topic='integration-test', value=message)

    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError as e:
        print(e)
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
    