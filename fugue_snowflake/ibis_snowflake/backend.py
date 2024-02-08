from typing import Iterator, Tuple

import ibis.expr.datatypes as dt
from ibis.backends.snowflake import Backend as BaseBackend, SnowflakeType
from .._utils import temp_rand_str, normalize_name


class Backend(BaseBackend):
    def _metadata(self, query: str) -> Iterator[Tuple[str, dt.DataType]]:
        with self.begin() as con:
            database = normalize_name(self.current_database)
            schema = normalize_name(self.current_schema)
            table = normalize_name("IBIS_" + temp_rand_str())
            full_name = f"{database}.{schema}.{table}"
            create_sql = (
                f"CREATE TEMP TABLE {full_name} AS SELECT * FROM ({query}) LIMIT 0"
            )
            print(create_sql)
            con.exec_driver_sql(create_sql)
            result = con.exec_driver_sql(f"DESC VIEW {full_name}").mappings().all()

        for field in result:
            name = field["name"]
            type_string = field["type"]
            is_nullable = field["null?"] == "Y"
            yield name, SnowflakeType.from_string(type_string, nullable=is_nullable)

        print("schema DONE")
