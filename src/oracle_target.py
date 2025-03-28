import os
from typing import Dict, Text, Any, List, Tuple, Callable
import logging
import pytz

import oracledb

from .base import Target
from .transform import int_ms_to_date


class OracleTarget(Target):
    """Oracle Target"""

    connection_class = oracledb.connect

    def __init__(self, config: Dict[Text, Any]) -> None:
        super().__init__(config)
        if self.config.delta is not None:
            os.environ["DATA_INTERVAL_START"] = self.get_latest_timestamp_for_delta()

    @staticmethod
    def _oracle_connection() -> oracledb.Connection:
        return OracleTarget.connection_class(
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            dsn=os.environ["DB_DSN"],
        )

    def get_latest_timestamp_for_delta(self):
        delta_column = self.config.delta["column"]
        delta_table = self.config.delta.get("table") or self.config.table
        with self._oracle_connection() as con:
            with con.cursor() as cur:
                cur.execute(f"select max({delta_column}) from {delta_table}")
                dt = cur.fetchone()[0]
        if not dt:
            return "0"
        norwegian_tz = pytz.timezone("Europe/Oslo")
        localized_dt = norwegian_tz.localize(dt)
        utc_dt = localized_dt.astimezone(pytz.UTC)
        utc_timestamp_ms = str(int(utc_dt.timestamp() * 1000))
        return utc_timestamp_ms

    def get_kode67(self, batch: List[Dict[Text, Any]]) -> List[Tuple]:
        k6_conf = self.config.k6_filter
        if k6_conf is not None:
            timestamp_col = k6_conf.timestamp
            timestamp = int_ms_to_date(batch[-1][timestamp_col])
            person_identer = [msg.get(k6_conf.col) for msg in batch]

            # generating sequential sql bind variable names for the range of personidenter i batchen
            # example :1,:2,:3 etc
            sequential_bind_variable_names = [":" + str(i + 1) for i in range(len(person_identer))]
            # Hack to allow IN lists up to 99999 values
            # https://stackoverflow.com/questions/4722220/sql-in-clause-1000-item-limit
            in_bind_names = ",".join([f"(1,{i})" for i in sequential_bind_variable_names])

            bind_values = dict(zip(sequential_bind_variable_names, person_identer))
            bind_values.update({"timestamp": timestamp})

            sql = f"""
                SELECT {k6_conf.filter_col}
                FROM {k6_conf.filter_table}
                WHERE (1,{k6_conf.filter_col}) IN ({in_bind_names})
                AND TRUNC(:timestamp) BETWEEN gyldig_fra_dato AND gyldig_til_dato
                AND skjermet_kode IN(6,7)
            """
            with self._oracle_connection() as con:
                with con.cursor() as cur:
                    return cur.execute(sql, bind_values).fetchall()
        return []

    def write_batch(self, batch: List[Dict[Text, Any]], transform: Callable) -> None:
        table = self.config.table
        if not batch:
            return

        k6_conf = self.config.k6_filter
        if k6_conf:
            kode67_personer = set(*zip(*self.get_kode67(batch)))
            for msg in batch:
                if msg.get(k6_conf.col) in kode67_personer:
                    msg["kafka_message"] = None

        transform.set_batch_time()
        batch = list(map(transform, batch))
        columns = list(batch[0].keys())
        sql = f"insert into {table} ({','.join(columns)}) select :{',:'.join(columns)} from dual where 1=1"

        duplicate_column = self.config.skip_duplicates_with
        if duplicate_column:
            duplicate_columns = [f"{item}=:{item}" for item in duplicate_column]
            bind_duplicate_column_names = " and ".join(duplicate_columns)
            sql += f""" and not exists ( select null from {table} where 
            {bind_duplicate_column_names} )"""

        with self._oracle_connection() as con:
            try:
                logging.info(f"Starting insert of batch")
                with con.cursor() as cur:
                    cur.setinputsizes(
                        **self.get_kv_from_config_by_method("oracledb.Cursor.setinputsizes")
                    )
                    cur.executemany(sql, batch)
                con.commit()
                logging.info(f"Batch of size {len(batch)} inserted")
            except oracledb.DatabaseError as e:
                (error,) = e.args
                logging.error(f"oracle code: {error.code}")
                logging.error(f"oracle message: {error.message}")
                logging.error(f"oracle context: {error.context}")
                logging.error(f"oracle sql statement: {sql}")
                logging.error(f"oracle batch of size: {len(batch)}")
                raise
