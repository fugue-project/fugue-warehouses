from typing import Any, List, Optional

import pandas as pd
import pyarrow as pa
from fugue import (
    AnyDataFrame,
    ArrowDataFrame,
    DataFrame,
    LocalBoundedDataFrame,
    LocalDataFrame,
)
from fugue_ibis import IbisDataFrame, IbisSchema, IbisTable
from triad import Schema

from ._utils import alter_table_columns, to_schema
from .client import SnowflakeClient


class SnowflakeDataFrame(IbisDataFrame):
    def __init__(self, table: IbisTable, schema: Any = None):
        super().__init__(table, schema)
        self._table_ref = None

    def to_sql(self) -> str:
        return self.native.compile()

    @property
    def is_local(self) -> bool:
        return False

    def _to_schema(self, schema: IbisSchema) -> Schema:
        return to_schema(schema)

    def _alter_table_columns(self, table: IbisTable, new_schema: Schema) -> IbisTable:
        return alter_table_columns(table, new_schema)

    def _to_new_df(self, table: IbisTable, schema: Any = None) -> DataFrame:
        return SnowflakeDataFrame(table, schema=schema)

    def _to_local_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        return ArrowDataFrame(table.execute(), schema=schema)

    def _to_iterable_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        return self._to_local_df(table, schema=schema)

    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> LocalBoundedDataFrame:
        return _ibis_head(self.native, n=n, columns=columns, tb=self._table_ref)

    def count(self) -> int:
        if self._table_ref is not None and self._table_ref.num_rows is not None:
            return self._table_ref.num_rows
        return super().count()

    def as_pandas(self) -> pd.DataFrame:
        return self.native.execute()


def _ibis_head(
    df: IbisTable,
    n: int,
    columns: Optional[List[str]] = None,
) -> LocalBoundedDataFrame:
    if columns is not None:
        df = df[columns]
    res = df.limit(n)
    return ArrowDataFrame(res.execute(), schema=to_schema(res.schema()))
