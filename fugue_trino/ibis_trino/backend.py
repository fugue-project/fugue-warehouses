from typing import Iterator, Tuple

import ibis.expr.datatypes as dt
import toolz
from ibis import util
from ibis.backends.trino import Backend as BaseBackend
from ibis.backends.trino.datatypes import parse


class Backend(BaseBackend):
    name = "fugue_trino"

    def _metadata(self, query: str) -> Iterator[Tuple[str, dt.DataType]]:
        tmpname = f"_ibis_trino_output_{util.guid()[:6]}"
        with self.begin() as con:
            con.exec_driver_sql(f"PREPARE {tmpname} FROM {query}")
            for name, _type in toolz.pluck(  # type: ignore
                ["Column Name", "Type"],
                con.exec_driver_sql(f"DESCRIBE OUTPUT {tmpname}").mappings(),
            ):
                if _type.lower().startswith("char(") or _type.lower().startswith(
                    "varchar("
                ):
                    ibis_type = dt.string
                elif _type.lower().startswith("timestamp") and _type.lower().endswith(
                    " with time zone"
                ):
                    # ibis_type = partial(dt.timestamp, timezone="UTC")
                    ibis_type = dt.timestamp
                else:
                    ibis_type = parse(_type)
                yield name, ibis_type(nullable=True)
            con.exec_driver_sql(f"DEALLOCATE PREPARE {tmpname}")
