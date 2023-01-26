import pytest
import hashlib
from kafka_source import KafkaSource


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
def test_json_deserializer():
    x = "{\"x\": \"x\"}".encode("utf-8")
    y = {"kafka_message": "{\"x\": \"x\"}", "x": "x"}
    assert KafkaSource._json_deserializer(x) == (
        y,
        "ed298bd5a15cbb33bc4b86650cda3376babf454119b172e107b7ac2e32f69789",
    )


@pytest.mark.unit
def test_avro_deserializer():
    pass
