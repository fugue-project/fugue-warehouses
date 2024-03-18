from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import fugue.api as fa
import fugue.plugins as fp
import ibis
import pandas as pd
import pyarrow as pa
import sqlglot
from fugue import (
    ArrowDataFrame,
    DataFrame,
    ExecutionEngine,
    LocalDataFrame,
    MapEngine,
    NativeExecutionEngine,
    PandasDataFrame,
    PartitionCursor,
    PartitionSpec,
    SQLEngine,
)
from fugue.constants import KEYWORD_PARALLELISM, KEYWORD_ROWCOUNT
from fugue.dataframe.utils import get_join_schemas
from fugue_ibis import IbisDataFrame, IbisExecutionEngine, IbisSQLEngine, IbisTable
from fugue_ibis.execution_engine import _JOIN_RIGHT_SUFFIX, IbisMapEngine
from triad import Schema, assert_or_throw

from ._constants import FUGUE_SF_CONF_CASE_SENSITIVE, FUGUE_SF_CONF_PACKAGES
from ._utils import quote_name
from .client import SnowflakeClient
from .dataframe import SnowflakeDataFrame

_FUGUE_PARTITION_KEY = "_fugue_partition_key"


@fp.transpile_sql.candidate(
    lambda sql, from_dialect, to_dialect: to_dialect == "snowflake"
)
def _transpile_to_sf(sql: str, from_dialect: str, to_dialect: str) -> str:
    if from_dialect == "snowflake":
        return sql
    case_sensitive = fa.get_current_conf().get(FUGUE_SF_CONF_CASE_SENSITIVE, True)
    return " ".join(
        sqlglot.transpile(
            sql, read=from_dialect, write=to_dialect, identify=case_sensitive
        )
    )


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

    def _dummy_window_order_by(self) -> str:
        return "ORDER BY 1"

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

    def query_to_table(self, statement: str, dfs: Dict[str, Any]) -> IbisTable:
        cte: List[str] = []
        for k, v in dfs.items():
            idf = self.to_df(v)
            cte.append(k + " AS (" + idf.to_sql() + ")")  # type: ignore
        if len(cte) > 0:
            sql = "WITH " + ",\n".join(cte) + "\n" + statement
        else:
            sql = statement
        return self.client.query_or_table_to_ibis(sql)

    def encode_column_name(self, name: str) -> str:
        return quote_name(name)

    def get_temp_table_name(self) -> str:
        return self.client.table_to_full_name(super().get_temp_table_name().upper())

    def to_df(self, df: Any, schema: Any = None) -> IbisDataFrame:
        if isinstance(df, SnowflakeDataFrame):
            assert_or_throw(
                schema is None,
                ValueError("schema must be None when df is SnowflakeDataFrame"),
            )
            return df
        if isinstance(df, DataFrame):
            res = self._register_df(
                df, schema=schema if schema is not None else df.schema
            )
            if df.has_metadata:
                res.reset_metadata(df.metadata)
            return res
        if isinstance(df, pa.Table):
            return self._register_df(ArrowDataFrame(df), schema=schema)
        if isinstance(df, pd.DataFrame):
            return self._register_df(PandasDataFrame(df, schema=schema), schema=schema)
        if isinstance(df, IbisTable):
            return SnowflakeDataFrame(df, schema=schema)
        if isinstance(df, Iterable):
            adf = ArrowDataFrame(df, schema)
            xdf = self._register_df(adf, schema=schema)
            return xdf
        raise NotImplementedError

    def table_exists(self, table: str) -> bool:
        return self.client.table_exists(table)

    def save_table(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        partition_spec: Optional[PartitionSpec] = None,
        **kwargs: Any,
    ) -> None:
        tb = self.client.table_to_full_name(table)
        if isinstance(df, SnowflakeDataFrame):
            self.client.ibis_to_table(df.native, tb, mode=mode)
        else:
            self.client.df_to_table(df, tb, mode=mode, engine=self.execution_engine)

    def load_table(self, table: str, **kwargs: Any) -> DataFrame:
        return SnowflakeDataFrame(self.client.query_or_table_to_ibis(table))

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
        suffixes = dict(lname="", rname="{name}" + _JOIN_RIGHT_SUFFIX)  # noqa
        if how.lower() in ["semi", "left_semi"]:
            tb = _df1.native.inner_join(_filter, on_fields, **suffixes)
        else:
            tb = _df1.native.left_join(_filter, on_fields, **suffixes)
            tb = tb[tb[key_schema.names[0] + _JOIN_RIGHT_SUFFIX].isnull()]
        return self.to_df(tb[end_schema.names], schema=end_schema)

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:
        if isinstance(df, SnowflakeDataFrame):
            tb = self.client.ibis_to_temp_table(df.native)
            return SnowflakeDataFrame(self.client.query_or_table_to_ibis(tb))
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
        assert_or_throw(n is None, NotImplementedError("n is not supported"))

        idf = self.to_df(df)
        tdf = idf.native.sample(fraction=frac, seed=seed)
        return SnowflakeDataFrame(tdf)

    def _register_df(
        self, df: DataFrame, name: Optional[str] = None, schema: Any = None
    ) -> SnowflakeDataFrame:
        tbn = self.client.df_to_temp_table(df, engine=self.execution_engine)
        return SnowflakeDataFrame(
            self.client.query_or_table_to_ibis(tbn), schema=schema
        )


class SnowflakeMapEngine(IbisMapEngine):
    def to_df(self, df: Any, schema: Any = None) -> IbisDataFrame:
        return self.execution_engine.sql_engine.to_df(df, schema)

    def map_dataframe(  # noqa: C901
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
        map_func_format_hint: Optional[str] = None,
    ) -> DataFrame:
        _output_schema = Schema(output_schema)
        _df = self.to_df(df)
        client: SnowflakeClient = self.execution_engine._client  # type: ignore
        udtf = client.register_udtf(
            _df,
            map_func,
            _output_schema,
            partition_spec,
            on_init,
            additional_packages=self.conf.get(FUGUE_SF_CONF_PACKAGES, ""),
        )
        dot = client.ibis_to_query_or_table(_df.native)
        input_cols = ",".join([quote_name(x) for x in _df.columns])
        order_by = ""
        if len(partition_spec.presort) > 0:
            order_by = " ORDER BY " + ", ".join(
                [
                    quote_name(x) + (" ASC" if asc else " DESC")
                    for x, asc in partition_spec.presort.items()
                ]
            )
        algo = partition_spec.algo.lower()
        default_partition_num = self.execution_engine.get_current_parallelism() * 4
        if len(partition_spec.partition_by) > 0:
            partitions_cols = ",".join(
                [quote_name(x) for x in partition_spec.partition_by]
            )
            if algo != "coarse":
                query = f"""SELECT b.* FROM
                ({dot}) AS a,
                TABLE({udtf}({input_cols})
                    OVER (PARTITION by {partitions_cols} {order_by})
                ) AS b"""
            else:
                num = self._get_partition_num(
                    partition_spec, _df, default_partition_num
                )
                part = f"ABS(HASH({partitions_cols}) % {num})"
                query = self._udtf_template(part, dot, udtf, input_cols, order_by)
        else:  # no partition key
            if (
                algo == "default"
            ):  # TODO: snowflake doesn't support as-is map_partitions
                algo = "hash"
            if algo == "even":
                if partition_spec.num_partitions.upper() == KEYWORD_ROWCOUNT:
                    part = "SEQ8()"
                else:
                    num = self._get_partition_num(partition_spec, _df, 0)
                    if num == 0:
                        part = "SEQ8()"
                    else:
                        part = f"SEQ8() % {num}"
            elif algo == "hash":
                num = self._get_partition_num(
                    partition_spec, _df, default_partition_num
                )
                part = f"ABS(HASH(*) % {num})"
            elif algo == "rand":
                num = self._get_partition_num(
                    partition_spec, _df, default_partition_num
                )
                part = f"ABS(RANDOM() % {num})"

            else:
                raise NotImplementedError(f"algo {algo} is not supported")
            query = self._udtf_template(part, dot, udtf, input_cols, order_by)

        it = client.query_or_table_to_ibis(query)
        return SnowflakeDataFrame(it)

    def _get_partition_num(
        self, partition_spec: PartitionSpec, df: DataFrame, zero_default: int
    ) -> Dict[str, Any]:
        _pf = lambda: self.execution_engine.get_current_parallelism()  # noqa
        params = {
            KEYWORD_ROWCOUNT: lambda: df.count(),
            KEYWORD_PARALLELISM: _pf,
        }
        num = partition_spec.get_num_partitions(**params)
        return num if num > 0 else zero_default

    def _udtf_template(
        self, partition: str, dot: str, udtf: str, input_cols: str, order_by: str
    ) -> str:
        return f"""SELECT b.* FROM
        (SELECT *, {partition} AS {_FUGUE_PARTITION_KEY} FROM ({dot})) AS a,
        TABLE({udtf}({input_cols})
            OVER (PARTITION by {_FUGUE_PARTITION_KEY} {order_by})
        ) AS b"""


class SnowflakeExecutionEngine(IbisExecutionEngine):
    _is_snowflake_engine = True

    def __init__(self, client: Optional[SnowflakeClient] = None, conf: Any = None):
        super().__init__(conf)
        self._client = (
            SnowflakeClient.get_or_create(self.conf) if client is None else client
        )
        self._parallelism = 0

    def create_non_ibis_execution_engine(self) -> ExecutionEngine:
        return NativeExecutionEngine(self.conf)

    def create_default_sql_engine(self) -> SQLEngine:
        return SnowflakeSQLEngine(self, self._client)

    def create_default_map_engine(self) -> MapEngine:
        return SnowflakeMapEngine(self)

    @property
    def is_distributed(self) -> bool:
        return True

    def __repr__(self) -> str:
        return "SnowflakeExecutionEngine"

    def get_current_parallelism(self) -> int:
        return self._client.get_current_parallelism()

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
