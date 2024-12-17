import src.environment

from ..transform import Transform
from ..mapping import Mapping

src.environment.isNotLocal = False


def test_run_subscribe(kafka_source, oracle_target, transform_config):
    transform = Transform(transform_config)
    mapping = Mapping(kafka_source, oracle_target, transform)

    mapping.run()


def test_consumer(consumer):

    m1 = consumer.poll()
    print(m1.value())


def test_oracle(oracle_target):
    with oracle_target._oracle_connection() as con:
        with con.cursor() as cur:
            table_name = oracle_target.config.table
            cur.execute(f"select kafka_key, kafka_topic from {table_name}")
            r = cur.fetchone()
    assert r[0] == "key0"
    assert r[1] == "test_topic"
