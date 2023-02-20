from typing import Any

import fugue.plugins as fp
import pyarrow as pa
from fugue import ArrowDataFrame, DataFrame, LocalDataFrame
from fugue_ibis import IbisDataFrame, IbisTable

from ._utils import is_trino_ibis_table, to_schema


class TrinoDataFrame(IbisDataFrame):
    def _to_new_df(self, table: IbisTable, schema: Any = None) -> DataFrame:
        return TrinoDataFrame(table, schema=schema)

    def _to_local_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        df = table.execute()
        schema = to_schema(table.schema())
        return ArrowDataFrame(df, schema=schema)

    def _to_iterable_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        return self._to_local_df(table, schema=schema)

    def _type_equal(self, tp1: pa.DataType, tp2: pa.DataType) -> bool:
        if pa.types.is_integer(tp1) and pa.types.is_integer(tp2):
            return True
        if pa.types.is_floating(tp1) and pa.types.is_floating(tp2):
            return True
        return tp1 == tp2


@fp.as_fugue_dataset.candidate(lambda df, **kwargs: is_trino_ibis_table(df))
def _trino_ibis_as_fugue(df: IbisTable, **kwargs: Any) -> TrinoDataFrame:
    return TrinoDataFrame(df, **kwargs)
