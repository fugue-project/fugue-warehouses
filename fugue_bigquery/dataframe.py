from typing import Any, List, Optional

import fugue.plugins as fp
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
from google.cloud.bigquery.table import Table as BigQueryTable
from triad import Schema

from ._utils import alter_table_columns, is_bq_ibis_table, to_schema
from .client import BigQueryClient


class BigQueryDataFrame(IbisDataFrame):
    def __init__(self, table: IbisTable, schema: Any = None):
        super().__init__(table, schema)
        self._table_ref = BigQueryClient.get_current().get_src_ref(table)

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
        return BigQueryDataFrame(table, schema=schema)

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


@fp.as_fugue_dataset.candidate(lambda df, **kwargs: is_bq_ibis_table(df))
def _bq_ibis_as_fugue(df: IbisTable, **kwargs: Any) -> BigQueryDataFrame:
    return BigQueryDataFrame(df, **kwargs)


@fp.head.candidate(lambda df, *args, **kwargs: is_bq_ibis_table(df))
def _bq_ibis_head(
    df: IbisTable,
    n: int,
    columns: Optional[List[str]] = None,
    as_fugue: bool = False,
) -> AnyDataFrame:
    df = _ibis_head(df, n=n, columns=columns)
    return df if as_fugue else df.as_pandas()


@fp.as_pandas.candidate(lambda df, *args, **kwargs: is_bq_ibis_table(df))
def _bq_ibis_as_pandas(df: IbisTable) -> pd.DataFrame:
    return df.execute()


@fp.as_arrow.candidate(lambda df, *args, **kwargs: is_bq_ibis_table(df))
def _bq_ibis_as_arrow(df: IbisTable) -> pa.Table:
    return pa.Table.from_pandas(df.execute())


def _ibis_head(
    df: IbisTable,
    n: int,
    columns: Optional[List[str]] = None,
    tb: Optional[BigQueryTable] = None,
) -> LocalBoundedDataFrame:
    client = BigQueryClient.get_current()
    if tb is None:
        tb = client.get_src_ref(df)
    if tb is None:
        if columns is not None:
            df = df[columns]
        res = df.limit(n)
        return ArrowDataFrame(res.execute(), schema=to_schema(res.schema()))
    else:
        fields = list(tb.schema)
        if columns is not None:
            df = df[columns]
            fields = [f for f in fields if f.name in columns]
        rows = client.bq.list_rows(tb, selected_fields=fields, max_results=n)
        return ArrowDataFrame(rows.to_dataframe(), schema=to_schema(df.schema()))
