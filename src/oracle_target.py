import os
from typing import Dict, Text, Any, List, Tuple
import oracledb
from base import Target
from transform import int_ms_to_date
import json


class OracleTarget(Target):
    """Oracle Target"""

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
        json_batch = [json.loads(msg["kafka_message"]) for msg in batch]
        if k6_conf is not None:
            timestamp_col = k6_conf["timestamp"]
            timestamp = int_ms_to_date(batch[-1][timestamp_col])
            timestamp_bind_value = {timestamp_col: timestamp}
            personer = [msg[k6_conf["col"]] for msg in json_batch]

            bind_names = [":" + str(i + 1) for i in range(len(personer))]
            in_bind_names = ",".join(bind_names)
            bind_values = dict(zip(bind_names, personer))
            bind_values.update(timestamp_bind_value)

            sql = f"""
                SELECT {k6_conf["filter-col"]}
                FROM {k6_conf["filter-table"]}
                WHERE {k6_conf["filter-col"]} IN ({in_bind_names})
                AND TRUNC(:{k6_conf["timestamp"]}) BETWEEN gyldig_fra_dato AND gyldig_til_dato
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
                sql += f""" and not exists ( select null from {table} where 
                {duplicate_column} = :{duplicate_column} )"""

        with self._oracle_connection() as con:
            with con.cursor() as cur:
                cur.setinputsizes(
                    **self.get_kv_from_config_by_method(
                        "cx_Oracle.Cursor.setinputsizes"
                    )
                )
                cur.executemany(sql, batch)
            con.commit()
