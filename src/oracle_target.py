import os
from typing import Dict, Text, Any, List, Tuple
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

            bind_values = dict(
                zip(sequential_bind_variable_names, person_identer))
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
            sql = f"insert into {table} ({','.join(columns)}, lastet_dato) select :{',:'.join(columns)}, sysdate from dual where 1=1"

            duplicate_column = self.config.get("skip-duplicates-with")
            if duplicate_column is not None:
                sql += f""" and not exists ( select null from {table} where 
                {duplicate_column} = :{duplicate_column} )"""

        with self._oracle_connection() as con:
            try:
                with con.cursor() as cur:
                    logging.debug(f"oracledb.is_thin_mode(): {con.thin}")
                    cur.setinputsizes(
                        **self.get_kv_from_config_by_method('oracledb.Cursor.setinputsizes'))
                    cur.executemany(sql, batch)
                con.commit()
            except oracledb.DatabaseError as e:
                (error,) = e.args
                logging.error(f"oracle code: {error.code}")
                logging.error(f"oracle message: {error.message}")
                logging.error(f"oracle context: {error.context}")
                logging.error(f"oracle sql statement: {sql}")
                logging.error(f"oracle insert metadata: {kafkaMetadata(batch)}")
                raise

def kafkaMetadata(batch):
    '''
    Takes a batch of kafka source data and logs the metadata. That is all the
    columns with names starting with 'kafka_'. The other colmns are ignored.
    Assumes that no columns starting with 'kafka_' contains any personal information.

            Parameters:
                    batch (List[Dict[Text, Any]]): A batch of kafka data
    '''
    header = [header for header in batch[0].keys() if "message" not in header]
    header_str = ''.join([f"{key:>30}" for key in header])
    log_list = [header_str]
    for row in batch:
        row_str = ''.join([f"{row[key]:>30}" for key in header])
        log_list.append(row_str)
    log_str = '\n'.join(log_list)
    return log_str