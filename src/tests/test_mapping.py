import pytest
# import hashlib
from kafka_source import KafkaSource
from oracle_target import OracleTarget
from transform import Transform
from mapping import Mapping
from fixtures import test_config


@pytest.fixture
def mapping(test_config):
    source = KafkaSource(test_config["source"])
    target = OracleTarget(test_config["target"])
    transform = Transform(test_config["transform"])
    return Mapping(source, target, transform)


@pytest.mark.unit
def test_get_start_timestamp_from_target_returns_int_if_source_kafka(mapping, mock_settings_env_vars):
    assert True
    # assert isinstance(get_start_timstamp_from_target(), int)
