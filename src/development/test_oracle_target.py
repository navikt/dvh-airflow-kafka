import pytest
from datetime import datetime
import json

from ..oracle_target import OracleTarget
from ..transform import Transform
from ..config import K6Filter
from .conftest import create_table, drop_table, table_insert
import hashlib


@pytest.fixture(autouse=True, scope="module")
def k67_filter():
    return {
        "filter-table": "person_ident",
        "filter-col": "person_id",
        "col-keypath-separator": ".",
        "col": "person_id",  # test nested: "person.id"
        "timestamp": "kafka_timestamp",
    }


@pytest.fixture(autouse=True, scope="module")
def oracle_target(k67_filter, table_name) -> OracleTarget:
    config = {
        "type": "oracle",
        "table": table_name,
        "k6-filter": k67_filter,
    }
    return OracleTarget(config=config)


@pytest.fixture(scope="module")
def kode67_id_list():
    return [i for i in range(10, 101, 10)]


@pytest.fixture(scope="module")
def transform(transform_config):
    return Transform(transform_config)


@pytest.fixture(autouse=True, scope="module")
def setup_person_table(oracle_target: OracleTarget, kode67_id_list):

    k6_filter = oracle_target.config.k6_filter
    create_table(
        k6_filter.filter_table,
        columns=[
            f"{k6_filter.filter_col} NUMBER",
            "gyldig_fra_dato DATE",
            "gyldig_til_dato DATE",
            "skjermet_kode NUMBER",
        ],
    )
    person_data = []
    for i in kode67_id_list:
        person_data.append(
            {
                k6_filter.filter_col: i,
                "gyldig_fra_dato": datetime(1900, 1, 1),
                "gyldig_til_dato": datetime(9999, 12, 31),
                "skjermet_kode": 6 if i < 51 else 7,
            }
        )
    table_insert(table_name=k6_filter.filter_table, data=person_data)

    yield

    drop_table(k6_filter.filter_table)


def test_get_kode67(oracle_target: OracleTarget):

    # det burde være 10 kode 6/7 personer i denne daten
    data = [
        {
            "person_id": i,
            "person": {"id": i},  # test nested
            "kafka_timestamp": int(datetime(2025, 1, 1).timestamp()),
            "kafka_message": json.dumps(f"value_{i}"),
        }
        for i in range(1900)
    ]

    kode67 = oracle_target.get_kode67(data)
    assert len(kode67) == 10


def test_write_batch_with_kode67(oracle_target: OracleTarget, transform: Transform, kode67_id_list):

    # det burde være 10 kode 6/7 personer i denne daten

    data = []
    for i in range(5000):
        melding = {
            "person_id": i,
            "value": f"melding {i}",
        }  # test nested: {"person": {"id": i}, "value": f"melding {i}"}
        melding_json = json.dumps(melding)
        kafka_hash = hashlib.sha256(melding_json.encode("utf-8")).hexdigest()
        data.append(
            {
                "kafka_key": f"key {i}",
                "kafka_topic": "topic-name",
                "kafka_offset": i,
                "kafka_partition": 0,
                "kafka_timestamp": int(datetime(2025, 1, 1).timestamp()),
                "kafka_message": melding_json,
                "person_id": i,
                "kafka_hash": kafka_hash,
                **melding,
            }
        )

    oracle_target.write_batch(data, transform)

    query_list = ",".join([str(i) for i in kode67_id_list])
    with oracle_target._oracle_connection() as con:
        with con.cursor() as cur:
            table_name = oracle_target.config.table
            cur.execute(
                f"select kafka_key, kafka_offset, kafka_message from {table_name} where kafka_offset in ({query_list})"
            )
            r = cur.fetchall()
    assert len(r) == len(kode67_id_list)
    assert r[0][2] == None
    assert r[-1][2] == None
