from typing import Any

import fugue.plugins as fp
import pandas as pd
import pyarrow as pa
from fugue import DataFrame, LocalDataFrame, PandasDataFrame
from fugue_ibis import IbisDataFrame, IbisTable

from ._utils import is_trino_ibis_table, to_schema


class TrinoDataFrame(IbisDataFrame):
    def to_sql(self) -> str:
        return str(self.native.compile())

    @property
    def is_local(self) -> bool:
        return False

    def _to_new_df(self, table: IbisTable, schema: Any = None) -> DataFrame:
        return TrinoDataFrame(table, schema=schema)

    def _to_local_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        df = table.execute()
        schema = to_schema(table.schema())
        return PandasDataFrame(df, schema=schema)

    def _to_iterable_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        return self._to_local_df(table, schema=schema)

    def as_pandas(self) -> pd.DataFrame:
        return self.native.execute()

    def _type_equal_(self, tp1: pa.DataType, tp2: pa.DataType) -> bool:
        if pa.types.is_integer(tp1) and pa.types.is_integer(tp2):
            return True
        if pa.types.is_floating(tp1) and pa.types.is_floating(tp2):
            return True
        return tp1 == tp2


@fp.as_fugue_dataset.candidate(lambda df, **kwargs: is_trino_ibis_table(df))
def _trino_ibis_as_fugue(df: IbisTable, **kwargs: Any) -> TrinoDataFrame:
    return TrinoDataFrame(df, **kwargs)


@fp.as_pandas.candidate(lambda df, *args, **kwargs: is_trino_ibis_table(df))
def _trino_as_pandas(df: IbisTable) -> pd.DataFrame:
    return df.execute()


@fp.as_arrow.candidate(lambda df, *args, **kwargs: is_trino_ibis_table(df))
def _trino_as_arrow(df: IbisTable) -> pa.Table:
    return pa.Table.from_pandas(df.execute())
