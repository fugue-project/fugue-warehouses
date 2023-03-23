from typing import Any, Iterable, List, Optional, Union

import ibis
import pandas as pd
import pyarrow as pa
from fugue import (
    AnyDataFrame,
    ArrowDataFrame,
    DataFrame,
    ExecutionEngine,
    NativeExecutionEngine,
    PartitionSpec,
    SQLEngine,
)
from fugue.dataframe.utils import get_join_schemas
from fugue_ibis import IbisDataFrame, IbisExecutionEngine, IbisSQLEngine, IbisTable
from fugue_ibis.execution_engine import _JOIN_RIGHT_SUFFIX
from triad import assert_or_throw

from .client import TrinoClient
from .dataframe import TrinoDataFrame
from ._utils import is_trino_repr, is_trino_ibis_table


class TrinoSQLEngine(IbisSQLEngine):
    def __init__(
        self,
        execution_engine: "ExecutionEngine",
        client: Optional[TrinoClient] = None,
    ) -> None:
        super().__init__(execution_engine)
        self._client = (
            TrinoClient.get_or_create(self.conf) if client is None else client
        )

    @property
    def is_distributed(self) -> bool:
        return True

    @property
    def dialect(self) -> Optional[str]:
        return "trino"

    @property
    def client(self) -> TrinoClient:
        return self._client

    @property
    def backend(self) -> ibis.BaseBackend:
        return self.client.ibis

    def encode_column_name(self, name: str) -> str:
        return '"' + name.replace('"', '\\"') + '"'

    def get_temp_table_name(self) -> str:
        return str(self.client.to_table_name(super().get_temp_table_name()))

    def to_df(self, df: Any, schema: Any = None) -> IbisDataFrame:
        if isinstance(df, TrinoDataFrame):
            assert_or_throw(
                schema is None,
                ValueError("schema must be None when df is TrinoDataFrame"),
            )
            return df
        if isinstance(df, DataFrame):
            res = self._register_df(
                df, schema=schema if schema is not None else df.schema
            )
            if df.has_metadata:
                res.reset_metadata(df.metadata)
            return res
        if isinstance(df, (pa.Table, pd.DataFrame)):
            return self._register_df(df, schema=schema)
        if is_trino_ibis_table(df):
            return TrinoDataFrame(df, schema=schema)
        if is_trino_repr(df):
            return TrinoDataFrame(self._client.query_to_ibis(df[1]))
        if isinstance(df, Iterable):
            adf = ArrowDataFrame(df, schema)
            xdf = self._register_df(adf, schema=schema)
            return xdf
        raise NotImplementedError

    def table_exists(self, table: str) -> bool:
        tb = self.client.to_table_name(table)
        tables = self.backend.list_tables(database=tb.schema)
        return tb.table in tables

    def save_table(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        partition_spec: Optional[PartitionSpec] = None,
        **kwargs: Any,
    ) -> None:
        self.client.df_to_table(df, table, overwrite=mode == "overwrite")

    def load_table(self, table: str, **kwargs: Any) -> DataFrame:
        return TrinoDataFrame(self.client.query_to_ibis(table))

    def join(
        self, df1: DataFrame, df2: DataFrame, how: str, on: Optional[List[str]] = None
    ) -> DataFrame:
        _on = on or []
        if how.lower() not in ["semi", "left_semi", "anti", "left_anti"]:
            return super().join(df1, df2, how, _on)
        _df1 = self.to_df(df1)
        _df2 = self.to_df(df2)
        key_schema, end_schema = get_join_schemas(_df1, _df2, how=how, on=on)
        _filter = _df2.native[key_schema.names]
        on_fields = [_df1.native[k] == _filter[k] for k in key_schema]
        if how.lower() in ["semi", "left_semi"]:
            tb = _df1.native.inner_join(
                _filter, on_fields, suffixes=("", _JOIN_RIGHT_SUFFIX)
            )
        else:
            tb = _df1.native.left_join(
                _filter, on_fields, suffixes=("", _JOIN_RIGHT_SUFFIX)
            )
            tb = tb[tb[key_schema.names[0] + _JOIN_RIGHT_SUFFIX].isnull()]
        return self.to_df(tb[end_schema.names], schema=end_schema)

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:
        if isinstance(df, TrinoDataFrame):
            tbn = self.client.df_to_table(df)
            return TrinoDataFrame(self.client.query_to_ibis(tbn))
        return self.to_df(df)

    def sample(
        self,
        df: DataFrame,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        replace: bool = False,
        seed: Optional[int] = None,
    ) -> DataFrame:
        assert_or_throw(
            (n is None and frac is not None and frac >= 0.0)
            or (frac is None and n is not None and n >= 0),
            ValueError(
                f"one and only one of n and frac should be non-negative, {n}, {frac}"
            ),
        )
        idf = self.to_df(df)
        if frac is not None:
            sql = f"SELECT * FROM _temp TABLESAMPLE BERNOULLI ({frac*100})"
            return self.to_df(
                self.query_to_table(sql, {"_temp": idf}),
                schema=df.schema,
            )
        else:
            return self.to_df(idf.native.limit(n), schema=df.schema)

    def _register_df(
        self, df: AnyDataFrame, name: Optional[str] = None, schema: Any = None
    ) -> TrinoDataFrame:
        tbn = self.client.df_to_table(df, table=name)
        return TrinoDataFrame(self.client.query_to_ibis(tbn), schema=schema)


class TrinoExecutionEngine(IbisExecutionEngine):
    def __init__(self, client: Optional[TrinoClient] = None, conf: Any = None):
        super().__init__(conf)
        self._client = (
            TrinoClient.get_or_create(self.conf) if client is None else client
        )

    def create_non_ibis_execution_engine(self) -> ExecutionEngine:
        return NativeExecutionEngine(self.conf)

    def create_default_sql_engine(self) -> SQLEngine:
        return TrinoSQLEngine(self, self._client)

    def is_non_ibis(self, ds: Any) -> bool:
        return not isinstance(ds, (IbisDataFrame, IbisTable)) and not is_trino_repr(ds)

    @property
    def is_distributed(self) -> bool:
        return True

    def __repr__(self) -> str:
        return "TrinoExecutionEngine"

    def load_df(
        self,
        path: Union[str, List[str]],
        format_hint: Any = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> DataFrame:
        return self.non_ibis_engine.load_df(path, format_hint, columns, **kwargs)

    def save_df(
        self,
        df: DataFrame,
        path: str,
        format_hint: Any = None,
        mode: str = "overwrite",
        partition_spec: Optional[PartitionSpec] = None,
        force_single: bool = False,
        **kwargs: Any,
    ) -> None:
        return self.non_ibis_engine.save_df(
            df, path, format_hint, mode, partition_spec, force_single, **kwargs
        )
