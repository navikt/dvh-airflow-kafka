import hashlib
import json
import copy
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import pytest

from .conftest import create_table, drop_table, table_insert
from ..oracle_target import OracleTarget
from ..transform import Transform


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
    with oracle_target.oracle_connection() as con:
        with con.cursor() as cur:
            table_name = oracle_target.config.table
            cur.execute(
                f"select kafka_key, kafka_offset, kafka_message from {table_name} where kafka_offset in ({query_list})"
            )
            r = cur.fetchall()
    assert len(r) == len(kode67_id_list)
    assert r[0][2] == None
    assert r[-1][2] == None


@contextmanager
def create_lob_kafka_senario(transform_config: list[dict[str, Any]], blob_type: str):
    lob_transform_config = copy.deepcopy(transform_config.copy())
    kafka_message_config = next((c for c in lob_transform_config if c["src"] == "kafka_message"), None)
    assert kafka_message_config is not None
    kafka_message_config["datatype"] = blob_type
    if blob_type == "BLOB":
        kafka_message_config["fun"] = "str -> byte-utf8"

    table_name = "lob_kafka_message_table"
    columns = [f"{obj['dst']} {obj['datatype']}" for obj in lob_transform_config]
    create_table(table_name=table_name, columns=columns)

    oracle_config = {
        "type": "oracle",
        "table": table_name,
        "custom-config": [{
            "method": "oracledb.Cursor.setinputsizes",
            "name": "kafka_message",
            "value": f"oracledb.DB_TYPE_{blob_type}",
        }]
    }

    try:
        yield OracleTarget(config=oracle_config), Transform(lob_transform_config)
    finally:
        drop_table(table_name=table_name)


@pytest.mark.parametrize(
    "size_bytes,lob_type", [
        (32, "BLOB"),
        (32 * 1024 - 1, "BLOB"),
        (32 * 1024, "BLOB"),
        (1024 * 1024, "BLOB"),
        (32, "CLOB"),
        (32 * 1024 - 1, "CLOB"),
        (32 * 1024, "CLOB"),
        (1024 * 1024, "CLOB"),
        (32, "NCLOB"),
        (32 * 1024 - 1, "NCLOB"),
        (32 * 1024, "NCLOB"),
        (1024 * 1024, "NCLOB"),
    ]
)
def test_write_batch_with_large_message(transform_config, size_bytes: int, lob_type: str):
    with create_lob_kafka_senario(transform_config, lob_type) as (oracle_target, transform):
        # 4 byte utf-8 character (for å triggere spesialbehandling av clob/nclob i OracleTarget.write_batch)
        melding_str = "🚀" * (size_bytes // 4)
        # pad with 1 byte utf-8 character
        melding_str = melding_str + "x" * (size_bytes - len(melding_str.encode("utf-8")))
        melding_byte = melding_str.encode("utf-8")
        kafka_hash = hashlib.sha256(melding_byte).hexdigest()

        # Start with a message that is not a LOB. This is to test an edge case where Oracle v26 falsely infers
        # the data type of kafka_message as raw or varchar, even though we used setinputsizes to force it to be a LOB.
        # In this case, a later insert of a LOB will fail with DPY-3013: unsupported Python type LOB for database type DB_TYPE_RAW
        # We implemented a workaround, where the first record in the batch is always converted to a LOB.
        # Without this workaround, tests for sizes 32 and 32*1024-1 succeed, the others for larger sizes fail.
        # This however showed another bug which might lead to data loss, so we reverted to before oracle v26 and will investigate.
        # This test is kept to show that oracledb < 26 does not have this bug.
        record0 = {
            "kafka_key": f"key 0",
            "kafka_topic": "topic-name",
            "kafka_offset": 0,
            "kafka_partition": 0,
            "kafka_timestamp": int(datetime(2025, 1, 1).timestamp()),
            "kafka_message": "x",
            "person_id":0,
            "kafka_hash": kafka_hash,
        }
        record1 = {
            "kafka_key": f"key 1",
            "kafka_topic": "topic-name",
            "kafka_offset": 1,
            "kafka_partition": 0,
            "kafka_timestamp": int(datetime(2025, 1, 1).timestamp()),
            "kafka_message": melding_str,  # blir dekodet til bytes i transformen hvis lob_type == "BLOB"
            "person_id":1,
            "kafka_hash": kafka_hash,
        }

        oracle_target.write_batch([record0, record1], transform)

        with oracle_target.oracle_connection() as con:
            with con.cursor() as cur:
                table_name = oracle_target.config.table
                cur.execute(
                    f"select kafka_key, kafka_offset, kafka_message from {table_name}"
                )
                r = cur.fetchall()

                assert len(r) == 2
                assert r[0][0] == "key 0"
                assert r[0][1] == 0
                assert r[0][2].read() == (b"x" if lob_type == "BLOB" else "x")
                assert r[1][0] == "key 1"
                assert r[1][1] == 1
                assert r[1][2].read() == (melding_byte if lob_type == "BLOB" else melding_str)
