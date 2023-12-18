import pytest
import yaml
from base import Source, Target


@pytest.fixture
def base_config():
    yaml_config = """
    source:
      type: kafka
      batch-size: 10000
      batch-interval: 5
      topic: teampam.stilling-dvh-5
      group-id: dvh_arb_stilling_konsument
      schema: avro
      key-decoder: int-64
    target:  
      type: oracle
      custom-config:
      - method: oracledb.Cursor.setinputsizes
        name: kafka_timestamp
        value: oracledb.TIMESTAMP
      - method: oracledb.Cursor.setinputsizes
        name: kafka_message
        value: oracledb.DB_TYPE_CLOB
      delta:
        column: kafka_timestamp
        table: DVH_ARB_STILLING.RAA_ARBEIDSMARKED_STILLING_STROM
      table: DVH_ARB_STILLING.RAA_ARBEIDSMARKED_STILLING_STROM
      skip-duplicates-with: 
        - kafka_hash
    """
    return yaml.safe_load(stream=yaml_config)


@pytest.fixture
def source_config(base_config):
    return base_config["source"]


@pytest.fixture
def target_config(base_config):
    return base_config["target"]


def test_source_constructor(source_config):
    Source(source_config)


def test_target_constructor(target_config):
    Target(target_config)
