from typing import Any

import fugue.plugins as fp
import pandas as pd
import pyarrow as pa
from fugue import ArrowDataFrame, DataFrame, LocalDataFrame
from fugue_ibis import IbisDataFrame, IbisSchema, IbisTable
from triad import Schema

from ._utils import is_sf_ibis_table, to_schema
from .client import SnowflakeClient


class SnowflakeDataFrame(IbisDataFrame):
    def to_sql(self) -> str:
        client = SnowflakeClient.get_current()
        return client.ibis_to_query_or_table(self.native, force_query=True)

    @property
    def is_local(self) -> bool:
        return False

    def _to_schema(self, schema: IbisSchema) -> Schema:
        return to_schema(schema)

    # def _alter_table_columns(self, table: IbisTable, new_schema: Schema) -> IbisTable:
    #    return alter_table_columns(table, new_schema)

    def _to_new_df(self, table: IbisTable, schema: Any = None) -> DataFrame:
        return SnowflakeDataFrame(table, schema=schema)

    def _to_local_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        return ArrowDataFrame(_sf_ibis_as_arrow(table))

    def _to_iterable_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        return self._to_local_df(table, schema=schema)

    def as_pandas(self) -> pd.DataFrame:
        return _sf_ibis_as_pandas(self.native)

    def as_arrow(self) -> pa.Table:
        return _sf_ibis_as_arrow(self.native)


@fp.as_fugue_dataset.candidate(lambda df, **kwargs: is_sf_ibis_table(df))
def _sf_ibis_as_fugue(df: IbisTable, **kwargs: Any) -> SnowflakeDataFrame:
    return SnowflakeDataFrame(df, **kwargs)


@fp.as_pandas.candidate(lambda df, *args, **kwargs: is_sf_ibis_table(df))
def _sf_ibis_as_pandas(df: IbisTable) -> pd.DataFrame:
    return _sf_ibis_as_arrow(df).to_pandas()


@fp.as_arrow.candidate(lambda df, *args, **kwargs: is_sf_ibis_table(df))
def _sf_ibis_as_arrow(df: IbisTable) -> pa.Table:
    client = SnowflakeClient.get_current()
    return client.ibis_to_arrow(df)
