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
      poll-timeout: 1
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
      k6-filter:
        filter-table: dt_person.dvh_person_ident_off_id
        filter-col: off_id
        timestamp: kafka_timestamp
        col: personIdent
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


def test_target_config_k6filter(target_config):
    TargetConfig(**target_config)
    target_config["k6-filter"].pop("col")
    with pytest.raises(ValueError):
        TargetConfig(**target_config)
    target_config["k6-filter"] = {}
    with pytest.raises(ValueError):
        TargetConfig(**target_config)

    target_config["k6-filter"] = None
    TargetConfig(**target_config)


def test_target_config_no_custom_config(target_config):
    target_config.pop("custom-config")

    TargetConfig(**target_config)


def test_source_poll_config_default_is_10(source_config):
    source_config.pop("poll-timeout")
    config = SourceConfig(**source_config)
    assert config.poll_timeout == 10


def test_source_poll_alias_works(source_config):
    config = SourceConfig(**source_config)
    assert config.poll_timeout == 1
