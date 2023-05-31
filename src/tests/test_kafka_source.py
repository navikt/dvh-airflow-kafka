import pytest
from kafka_source import KafkaSource
from benedict import benedict


from fixtures.fixtures import test_config, mock_settings_env_vars, test_config_json


@pytest.fixture
def kafka_source(test_config, mock_settings_env_vars):
    return KafkaSource(test_config["source"])


@pytest.mark.integration
def test_seek_to_timestamp_infinty_should_return_offset_minus_1(kafka_source):
    tp = kafka_source.seek_to_timestamp(1981317520000)[0]
    assert tp.offset == - 1


@pytest.mark.integration
def test_seek_to_timestamp_0_should_return_offset_0(kafka_source):
    tp = kafka_source.seek_to_timestamp(0)[0]
    assert tp.offset == 0


@pytest.mark.unit
def test_key_deserializer():
    assert (
        KafkaSource._key_deserializer(b"Hello, world!") == "Hello, world!"
    )


@pytest.mark.unit
def test_string_deserializer():
    x = "Hello, world!".encode("utf-8")
    assert KafkaSource._string_deserializer(x) == (
        {"kafka_message": "\"Hello, world!\""},
        "315f5bdb76d078c43b8ac0064e4a0164612b1fce77c869345bfc94c75894edd3",
    )


@pytest.mark.unit
def test_json_deserializer(test_config_json):
    x = '{"x": "x"}'
    x_bytes = x.encode("utf-8")
    src = KafkaSource(test_config_json["source"])
    deserialized = src._json_deserializer(x_bytes)

    assert deserialized["kafka_hash"] == "ed298bd5a15cbb33bc4b86650cda3376babf454119b172e107b7ac2e32f69789"
    assert deserialized["kafka_message"] == x
    assert deserialized["x"] == "x"


@pytest.mark.unit
def test_avro_deserializer():
    pass
