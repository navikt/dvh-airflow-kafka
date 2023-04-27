import pytest
# import hashlib
from kafka_source import KafkaSource
from oracle_target import OracleTarget
from transform import Transform
from mapping import Mapping
from fixtures.fixtures import test_config


@pytest.fixture
def mapping(test_config):
    source = KafkaSource(test_config["source"])
    target = OracleTarget(test_config["target"])
    transform = Transform(test_config["transform"])
    return Mapping(source, target, transform)
