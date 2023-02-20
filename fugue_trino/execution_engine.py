from typing import Any, Iterable, List, Optional, Union

import ibis
import pyarrow as pa
from fugue import (
    ArrowDataFrame,
    DataFrame,
    ExecutionEngine,
    NativeExecutionEngine,
    PartitionSpec,
)
from fugue.dataframe.utils import get_join_schemas
from fugue_ibis import IbisDataFrame, IbisExecutionEngine, IbisTable
from fugue_ibis._utils import to_ibis_schema
from fugue_ibis.execution_engine import _JOIN_RIGHT_SUFFIX
from triad import assert_or_throw
from triad.utils.schema import quote_name

from .client import TrinoClient
from .dataframe import TrinoDataFrame


class TrinoExecutionEngine(IbisExecutionEngine):
    def __init__(self, client: Optional[TrinoClient] = None, conf: Any = None):
        super().__init__(conf)
        self._client = (
            TrinoClient.get_or_create(self.conf) if client is None else client
        )

    def create_non_ibis_execution_engine(self) -> ExecutionEngine:
        return NativeExecutionEngine(self.conf)

    @property
    def is_distributed(self) -> bool:
        return False

    @property
    def client(self) -> TrinoClient:
        return self._client

    @property
    def dialect(self) -> str:
        return "trino"

    @property
    def backend(self) -> ibis.BaseBackend:
        return self._client.ibis

    def encode_column_name(self, name: str) -> str:
        return quote_name(name, quote="'")

    def get_temp_table_name(self) -> str:
        return self.client.table_to_full_name(super().get_temp_table_name())

    def _to_ibis_dataframe(self, df: Any, schema: Any = None) -> IbisDataFrame:
        if isinstance(df, TrinoDataFrame):
            assert_or_throw(
                schema is None,
                ValueError("schema must be None when df is TrinoDataFrame"),
            )
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
            return TrinoDataFrame(df, schema=schema)
        if isinstance(df, Iterable):
            adf = ArrowDataFrame(df, schema)
            return self._register_df(adf.native, schema=schema)
        raise NotImplementedError

    def _to_non_ibis_dataframe(self, df: Any, schema: Any = None) -> DataFrame:
        return self.non_ibis_engine.to_df(df, schema)

    def __repr__(self) -> str:
        return "TrinoExecutionEngine"

    def _join(
        self, df1: DataFrame, df2: DataFrame, how: str, on: Optional[List[str]] = None
    ) -> DataFrame:
        _on = on or []
        if how.lower() not in ["semi", "left_semi", "anti", "left_anti"]:
            return super().join(df1, df2, how, _on)
        _df1 = self._to_ibis_dataframe(df1)
        _df2 = self._to_ibis_dataframe(df2)
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
        return self._to_ibis_dataframe(tb[end_schema.names], schema=end_schema)

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:
        if self.is_non_ibis(df):
            return self.non_ibis_engine.persist(df, lazy=lazy, **kwargs)

        if isinstance(df, TrinoDataFrame):
            sql = df.native.compile()
            tbn = self.client.query_to_table(sql)  # type: ignore
            parts = tbn.split(".")
            tb = self.backend.table(parts[2], database=parts[0] + "." + parts[1])
            res = TrinoDataFrame(tb)
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
        if self.is_non_ibis(df):
            return self.non_ibis_engine.sample(
                df, n=n, frac=frac, replace=replace, seed=seed
            )

        assert_or_throw(
            (n is None and frac is not None and frac >= 0.0)
            or (frac is None and n is not None and n >= 0),
            ValueError(
                f"one and only one of n and frac should be non-negative, {n}, {frac}"
            ),
        )
        idf = self._to_ibis_dataframe(df)
        if frac is not None:
            sql = f"SELECT * FROM _temp WHERE rand()<{frac}"
            return self._to_ibis_dataframe(
                self._raw_select(sql, {"_temp": idf}),
                schema=df.schema,
            )
        else:
            return self._to_ibis_dataframe(idf.native.limit(n), schema=df.schema)

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

    def table_exists(self, table: str) -> bool:
        tb = self.client.table_to_full_name(table).split(".")
        tables = self.backend.list_tables(database=tb[1])
        return tb[-1] in tables

    def save_table(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        partition_spec: Optional[PartitionSpec] = None,
        **kwargs: Any,
    ) -> None:
        tb = self.client.table_to_full_name(table).split(".")
        sb = self.client.connect_to_schema(tb[1])
        if mode == "overwrite":
            sb.drop_table(tb[-1], force=True)
        if isinstance(df, IbisDataFrame):
            sb.create_table(tb[-1], df.native)
        else:
            sb.create_table(tb[-1], df.as_pandas(), schema=to_ibis_schema(df.schema))

    def load_table(self, table: str, **kwargs: Any) -> DataFrame:
        tb = self.client.table_to_full_name(table).split(".")
        return TrinoDataFrame(self.backend.table(tb[-1], schema=tb[1]))

    def _register_df(
        self, df: pa.Table, name: Optional[str] = None, schema: Any = None
    ) -> TrinoDataFrame:
        tbn = self.client.df_to_table(df)
        parts = tbn.split(".")
        tb = self.backend.table(parts[-1], schema=parts[-2])
        return TrinoDataFrame(tb, schema=schema)
