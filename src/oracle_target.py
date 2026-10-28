import os
from typing import Dict, Text, Any, List, Tuple, Callable
import logging
import pytz

import oracledb
from oracledb import Connection

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
    def oracle_connection() -> oracledb.Connection:
        return OracleTarget.connection_class(
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            dsn=os.environ["DB_DSN"],
        )

    def get_latest_timestamp_for_delta(self):
        delta_column = self.config.delta["column"]
        delta_table = self.config.delta.get("table") or self.config.table
        with self.oracle_connection() as con:
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

    # Handles nested columns for person identifier, e.g. 'person.id'
    def get_person_identifier(self, msg: Dict[Text, Any], column: Text) -> Any:
        for col in column.split(self.config.k6_filter.col_keypath_separator):
            msg = msg.get(col)
            if msg is None:
                return None
        return msg

    def get_kode67(self, batch: List[Dict[Text, Any]]) -> List[Tuple]:
        k6_conf = self.config.k6_filter
        if k6_conf is not None:
            timestamp_col = k6_conf.timestamp
            timestamp = int_ms_to_date(batch[-1][timestamp_col])

            person_identer = [self.get_person_identifier(msg, k6_conf.col) for msg in batch]

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
            with self.oracle_connection() as con:
                with con.cursor() as cur:
                    return cur.execute(sql, bind_values).fetchall()
        return []

    def write_batch(self, batch: List[Dict[Text, Any]], transform: Callable) -> None:
        table = self.config.table
        if not batch:
            return

        # mask kafka messages for kode 6/7 persons
        k6_conf = self.config.k6_filter
        if k6_conf:
            kode67_personer = set(*zip(*self.get_kode67(batch)))
            for msg in batch:
                if self.get_person_identifier(msg, k6_conf.col) in kode67_personer:
                    msg["kafka_message"] = None

        # Apply transformations
        batch = list(map(transform, batch))
        columns = list(batch[0].keys())
        sql = f"insert into {table} ({','.join(columns)}) select :{',:'.join(columns)} from dual where 1=1"

        # If the user has specified columns to skip duplicates on, add a NOT EXISTS clause to the insert statement
        duplicate_column = self.config.skip_duplicates_with
        if duplicate_column:
            duplicate_columns = [f"{item}=:{item}" for item in duplicate_column]
            bind_duplicate_column_names = " and ".join(duplicate_columns)
            sql += f""" and not exists ( select null from {table} where 
            {bind_duplicate_column_names} )"""

        column_type_override = self.get_kv_from_config_by_method("oracledb.Cursor.setinputsizes")

        # LOB columns need special treatment, see following comment
        lob_columns = {
            column_name: column_type for column_name, column_type in column_type_override.items()
            if column_type in (oracledb.DB_TYPE_BLOB, oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NCLOB)
        }
        # Check if the columns specified in the custom config are actually present in the database record
        for column_name in lob_columns.keys():
            if column_name not in columns:
                raise KeyError(f"Error in custom config: {column_name} is not a database column")

        with self.oracle_connection() as con:
            try:
                if lob_columns:
                    # Since oracle version 26.0, large string or byte values are no longer converted to LOB types automatically.
                    # This leads to errors when the size exceeds 32kb. At the same time, strings are no longer converted to bytes automatically,
                    # when inserting into BLOB column. In this case, the user has to specify the byte encoding as part of the configuration file.
                    # Example:
                    #     src: kafka_message
                    #     dst: kafka_message
                    #     fun: str -> byte-utf8
                    # Note: this does not affect if oracle stores the lob within the row or outside of the row, the database
                    # handles this automatically.
                    for item in batch:
                        for column_name, lob_type in lob_columns.items():
                            self._create_temp_lob_if_needed(column_name, con, item, lob_type)

                logging.info(f"Starting insert of batch with {len(batch)} records")
                with con.cursor() as cur:
                    cur.setinputsizes(**column_type_override)
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

    @staticmethod
    def _create_temp_lob_if_needed(column_name, con: Connection, item: dict[str, Any], lob_type):
        """Create a temporary LOB if the value exceeds the size threshold.

        A temporary lob is created with con.createlob. This allocates a temporary lob in the database, which requires
        a roundtrip to the database. This is expensive, so we do everything to avoid that by checking the size
        of the value. The threshold is 32kb, below we can insert the value directly.
        """
        lob_value = item[column_name]
        if lob_type == oracledb.DB_TYPE_BLOB:
            if not isinstance(lob_value, bytes):
                raise ValueError(f"Column {column_name} is configured as BLOB, but the value is not bytes. "
                                 f"Please convert the value to bytes, e.g. with 'fun: \"str -> byte-utf8\"' in the transform config.")
            # bytes: len == number of bytes
            if len(lob_value) >= 1024 * 32:
                item[column_name] = con.createlob(lob_type, lob_value)
        elif lob_type in (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NCLOB):
            if not isinstance(lob_value, str):
                raise ValueError(f"Column {column_name} is configured as CLOB/NCLOB, but the value is not a string. "
                                 f"Please remove any 'fun: \"str -> byte-utf8\"' conversions from the transform config.")
            # str: len == number of characters, each taking between 1 and 4 bytes.
            if len(lob_value) >= 1024 * 32:
                item[column_name] = con.createlob(lob_type, lob_value)
            elif len(lob_value) >= 1024 * 32 / 4:  # 4 bytes per character is the worst case for UTF-8
                # Could be over 32kb in bytes, so convert to bytes and check length.
                # Takes some extra resources, but it is a lot cheaper than a roundtrip to the database
                # to create a temporary lob.
                lob_value_bytes = lob_value.encode("utf-8")
                if len(lob_value_bytes) >= 1024 * 32:
                    item[column_name] = con.createlob(lob_type, lob_value)
        else:
            raise NotImplementedError(f"Unsupported lob type {lob_type} for column {column_name}")

        # All fallthrough cases: if the value is below the threshold, we do nothing and insert it directly.