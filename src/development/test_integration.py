import pytest
import os
from datetime import datetime, timedelta

from ..transform import Transform
from ..mapping import Mapping

from .conftest import now, n_kafka_messages


def test_run_subscribe(kafka_source, oracle_target, transform_config):
    transform = Transform(transform_config)
    mapping = Mapping(kafka_source, oracle_target, transform)

    mapping.run()

    with oracle_target._oracle_connection() as con:
        with con.cursor() as cur:
            table_name = oracle_target.config.table
            cur.execute(f"select kafka_key, kafka_topic from {table_name}")
            r = cur.fetchone()
    assert r[0] == "key0"
    assert r[1] == "test_topic"


def test_run_assign(kafka_source_assign, oracle_target, transform_config):
    os.environ["DATA_INTERVAL_START"] = str(
        int(datetime.timestamp(now - timedelta(days=(n_kafka_messages))))
    )
    os.environ["DATA_INTERVAL_END"] = str(
        int(
            datetime.timestamp(now - timedelta(days=(n_kafka_messages - n_kafka_messages // 2 - 1)))
        )
    )

    transform = Transform(transform_config)
    mapping = Mapping(kafka_source_assign, oracle_target, transform)

    mapping.run()

    with oracle_target._oracle_connection() as con:
        with con.cursor() as cur:
            table_name = oracle_target.config.table
            cur.execute(f"select kafka_key, kafka_topic from {table_name}")
            r = cur.fetchone()
    assert r[0] == "key0"
    assert r[1] == "test_topic"
