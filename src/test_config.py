from .config import SourceConfig, TargetConfig
import pytest
import yaml


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


def test_source_config(source_config):
    conf = SourceConfig(**source_config)
    assert conf.batch_size == 10000
    assert conf.batch_interval == 5


def test_invalid_schema(source_config):
    source_config["schema"] = "xml"

    with pytest.raises(ValueError):
        SourceConfig(**source_config)


def test_target_config(target_config):
    target_config = TargetConfig(**target_config)

    assert target_config.type == "oracle"
    assert type(target_config.skip_duplicates_with) == list


def test_invalid_target_config(target_config):
    target_config["skip-duplicates-with"] = "kafka_hash"

    with pytest.raises(ValueError):
        TargetConfig(**target_config)
