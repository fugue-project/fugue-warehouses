from typing import Any, Iterable, List, Optional, Union

import ibis
import pyarrow as pa
from fugue import (
    ArrowDataFrame,
    DataFrame,
    ExecutionEngine,
    NativeExecutionEngine,
    PartitionSpec,
    SQLEngine,
)
from fugue.dataframe.utils import get_join_schemas
from fugue_ibis import IbisDataFrame, IbisExecutionEngine, IbisTable, IbisSQLEngine
from fugue_ibis.execution_engine import _JOIN_RIGHT_SUFFIX
from triad import assert_or_throw

from .client import SnowflakeClient
from .dataframe import SnowflakeDataFrame


class SnowflakeSQLEngine(IbisSQLEngine):
    def __init__(
        self,
        execution_engine: "ExecutionEngine",
        client: Optional[SnowflakeClient] = None,
    ) -> None:
        super().__init__(execution_engine)
        self._client = (
            SnowflakeClient.get_or_create(self.conf) if client is None else client
        )

    @property
    def is_distributed(self) -> bool:
        return True

    @property
    def dialect(self) -> Optional[str]:
        return "snowflake"

    @property
    def client(self) -> SnowflakeClient:
        return self._client

    @property
    def backend(self) -> ibis.BaseBackend:
        return self.client.ibis

    def encode_column_name(self, name: str) -> str:
        return "`" + name.replace("`", "\\`") + "`"

    def to_df(self, df: Any, schema: Any = None) -> SnowflakeDataFrame:
        if isinstance(df, SnowflakeDataFrame):
            return df
        if isinstance(df, DataFrame):
            res = self._register_df(
                df.as_arrow(), schema=schema if schema is not None else df.schema
            )
            if df.has_metadata:
                res.reset_metadata(df.metadata)
            return res
        if isinstance(df, pa.Table):
            return self._register_df(df, schema=schema)
        if isinstance(df, IbisTable):
            return SnowflakeDataFrame(df, schema=schema)
        if isinstance(df, Iterable):
            adf = ArrowDataFrame(df, schema)
            xdf = self._register_df(adf.native, schema=schema)
            return xdf
        raise NotImplementedError

    def table_exists(self, table: str) -> bool:
        tb = self.client.table_to_full_name(table).split(".")
        tables = self.backend.list_tables(database=tb[0] + "." + tb[1])
        return tb[-1] in tables

    def save_table(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        partition_spec: Optional[PartitionSpec] = None,
        **kwargs: Any,
    ) -> None:
        tb = self.client.table_to_full_name(table)
        if mode == "overwrite":
            self.client.bq.delete_table(tb, not_found_ok=True)
        if isinstance(df, IbisDataFrame):
            self.client.query_to_table(df.to_sql(), tb)
        else:
            self.client.arrow_to_table(df.as_arrow(), tb)

    def load_table(self, table: str, **kwargs: Any) -> DataFrame:
        tb = self.client.table_to_full_name(table)
        return SnowflakeDataFrame(self.client.table_to_ibis(tb))

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
        if isinstance(df, SnowflakeDataFrame):
            sql = df.native.compile()
            tbn = self.client.query_to_table(sql)
            parts = tbn.split(".")
            tb = self.backend.table(parts[2], database=parts[0] + "." + parts[1])
            res = SnowflakeDataFrame(tb)
            if df.has_metadata:
                res.reset_metadata(df.metadata)
            return res
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
            sql = f"SELECT * FROM _temp WHERE rand()<{frac}"
            return self.to_df(
                self.query_to_table(sql, {"_temp": idf}),
                schema=df.schema,
            )
        else:
            return self.to_df(idf.native.limit(n), schema=df.schema)

    def _register_df(
        self, df: pa.Table, name: Optional[str] = None, schema: Any = None
    ) -> SnowflakeDataFrame:
        tbn = self.client.arrow_to_table(df)
        tb = self.backend.table(tbn)
        return SnowflakeDataFrame(tb, schema=schema)


class SnowflakeExecutionEngine(IbisExecutionEngine):
    def __init__(self, client: Optional[SnowflakeClient] = None, conf: Any = None):
        super().__init__(conf)
        self._client = (
            SnowflakeClient.get_or_create(self.conf) if client is None else client
        )

    def create_non_ibis_execution_engine(self) -> ExecutionEngine:
        return NativeExecutionEngine(self.conf)

    def create_default_sql_engine(self) -> SQLEngine:
        return SnowflakeSQLEngine(self, self._client)

    @property
    def is_distributed(self) -> bool:
        return True

    def __repr__(self) -> str:
        return "SnowflakeExecutionEngine"

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
