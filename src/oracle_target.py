import os
from typing import Dict, Text, Any, List, Tuple
from benedict import benedict
import oracledb
from base import Target
from transform import int_ms_to_date
import json
import logging


class OracleTarget(Target):
    """Oracle Target"""

    oracledb.init_oracle_client()

    connection_class = oracledb.connect

    def _oracle_connection(self) -> oracledb.connect:
        return OracleTarget.connection_class(
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            dsn=os.environ["DB_DSN"],
            encoding="utf-8",
            nencoding="utf-8",
        )

    def get_kode67(self, batch: List[Dict[Text, Any]]) -> List[Tuple]:
        k6_conf = self.config.get("k6-filter")
        if k6_conf is not None:
            timestamp_col = k6_conf["timestamp"]
            timestamp = int_ms_to_date(batch[-1][timestamp_col])
            person_identer = [msg[k6_conf["col"]] for msg in batch]

            # generating sequential sql bind variable names for the range of personidenter i batchen
            # example :1,:2,:3 etc
            sequential_bind_variable_names = [
                ":" + str(i + 1) for i in range(len(person_identer))
            ]
            in_bind_names = ",".join(sequential_bind_variable_names)

            bind_values = dict(zip(sequential_bind_variable_names, person_identer))
            bind_values.update({"timestamp": timestamp})

            sql = f"""
                SELECT {k6_conf["filter-col"]}
                FROM {k6_conf["filter-table"]}
                WHERE {k6_conf["filter-col"]} IN ({in_bind_names})
                AND TRUNC(:timestamp) BETWEEN gyldig_fra_dato AND gyldig_til_dato
                AND skjermet_kode IN(6,7)
            """

            with self._oracle_connection() as con:
                with con.cursor() as cur:
                    return cur.execute(sql, bind_values).fetchall()
        return []

    def write_batch(self, batch: List[Dict[Text, Any]]) -> None:
        table = self.config["table"]

        fp = self.config.get("custom-insert")
        if fp:
            with open(fp) as f:
                sql = f.read()
        else:
            columns = list(batch[0].keys())
            sql = f"insert into {table} ({','.join(columns)}) select :{',:'.join(columns)} from dual where 1=1"

            duplicate_column = self.config.get("skip-duplicates-with")
            if duplicate_column is not None:
                duplicate_columns = [f"{item}=:{item}" for item in duplicate_column]
                bind_duplicate_column_names = " and ".join(duplicate_columns)
                sql += f""" and not exists ( select null from {table} where 
                {bind_duplicate_column_names} )"""

        with self._oracle_connection() as con:
            try:
                with con.cursor() as cur:
                    logging.debug(f"oracledb.is_thin_mode(): {con.thin}")
                    cur.setinputsizes(kafka_message=oracledb.BLOB)
                    cur.executemany(sql, batch)
                con.commit()
            except oracledb.DatabaseError as e:
                (error,) = e.args
                logging.error(f"oracle code: {error.code}")
                logging.error(f"oracle message: {error.message}")
                logging.error(f"oracle context: {error.context}")
                logging.error(f"oracle sql statement: {sql}")
                logging.error(f"oracle insert data: {batch}")
                raise
