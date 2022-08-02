import os
from typing import Dict, Text, Any, List
import cx_Oracle
from base import Target


class OracleTarget(Target):
    """Oracle Target"""

    connection_class = cx_Oracle.connect

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

            k6_conf = self.config.get("k6-filter")
            if k6_conf is not None:
                sql += f""" and not exists ( select null from {k6_conf["filter-table"]}
                where {k6_conf["filter-col"]} = :{k6_conf["col"]} and 
                trunc(:{k6_conf["timestamp"]}) between gyldig_fra_dato and gyldig_til_dato and
                skjermet_kode = 6)"""

        with OracleTarget.connection_class(
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            dsn=os.environ["DB_DSN"],
            encoding="utf-8",
            nencoding="utf-8",
        ) as con:
            with con.cursor() as cur:
                cur.setinputsizes(
                    **self.get_kv_from_config_by_method(
                        "cx_Oracle.Cursor.setinputsizes"
                    )
                )
                cur.executemany(sql, batch)
            con.commit()
