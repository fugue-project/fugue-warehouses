import os
from contextlib import contextmanager
from contextvars import ContextVar
from tempfile import TemporaryDirectory
from typing import Any, Iterator, List, Optional
from uuid import uuid4

import ibis
import pyarrow as pa
import snowflake.connector
from fugue import (
    ArrayDataFrame,
    ArrowDataFrame,
    DataFrame,
    ExecutionEngine,
    LocalDataFrame,
    PartitionSpec,
)
from fugue_ibis import IbisTable
from pyarrow.parquet import write_table as write_parquet
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.result_batch import ResultBatch
from triad import Schema, SerializableRLock, assert_or_throw

from ._constants import get_client_init_params
from ._utils import (
    get_arrow_from_batches,
    pa_type_to_snowflake_type_str,
    quote_name,
    to_schema,
    to_snowflake_schema,
)

_FUGUE_SNOWFLAKE_CLIENT_CONTEXT = ContextVar(
    "_FUGUE_SNOWFLAKE_CLIENT_CONTEXT", default=None
)

_CONTEXT_LOCK = SerializableRLock()


class SnowflakeClient:
    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        database: str,
        warehouse: str,
        schema: str,
        role: Optional[str] = "ACCOUNTADMIN",
    ):
        self._temp_tables: List[str] = []
        self._account = account
        self._user = user
        self._password = password
        self._warehouse = warehouse
        self._database = database
        self._schema = schema
        self._role = role

        self._ibis = ibis.snowflake.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=f"{database}/{schema}",
            role=role,
        )

        con = self._ibis.con.connect()
        self._sf: Any = con.connection.dbapi_connection  # type: ignore

    @staticmethod
    def get_or_create(conf: Any = None) -> "SnowflakeClient":
        with _CONTEXT_LOCK:
            res = _FUGUE_SNOWFLAKE_CLIENT_CONTEXT.get()
            if res is None:
                params = get_client_init_params(conf)
                res = SnowflakeClient(**params)
                _FUGUE_SNOWFLAKE_CLIENT_CONTEXT.set(res)  # type: ignore
            return res

    @staticmethod
    def get_current() -> "SnowflakeClient":
        with _CONTEXT_LOCK:
            res = _FUGUE_SNOWFLAKE_CLIENT_CONTEXT.get()
            assert_or_throw(
                res is not None, ValueError("no SnowflakeClient was initialized")
            )
            return res  # type: ignore

    @property
    def sf(self) -> snowflake.connector.SnowflakeConnection:
        return self._sf

    def __setstate__(self, state):
        for k, v in state.items():
            setattr(self, k, v)
        self._ibis = ibis.snowflake.connect(
            account=self._account,
            user=self._user,
            password=self._password,
            warehouse=self._warehouse,
            database=f"{self._database}/{self._schema}",
            role=self._role,
        )

        con = self._ibis.con.connect()
        self._sf: Any = con.connection.dbapi_connection  # type: ignore

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_sf"]
        del state["_ibis"]
        return state

    @contextmanager
    def cursor(self) -> Iterator[SnowflakeCursor]:
        with self._ibis.con.connect() as con:
            with con.connection.cursor() as cur:
                yield cur

    def stop(self):
        # for tt in self._temp_tables:
        #    self.sf.cursor().execute(f"DROP TABLE IF EXISTS {tt}")
        self.sf.close()

    def __enter__(self) -> "SnowflakeClient":
        return self

    def __exit__(
        self, exception_type: Any, exception_value: Any, exception_traceback: Any
    ) -> None:
        self.stop()

    def connect_to_schema(self, schema: str) -> Any:
        self.sf.cursor().execute(f"USE SCHEMA {schema}")

    @property
    def ibis(self) -> ibis.BaseBackend:
        return self._ibis

    def query_to_ibis(self, query: str) -> IbisTable:
        return self.ibis.sql(query)

    def query_to_result_batches(
        self, query: str, cursor: Optional[SnowflakeCursor] = None
    ) -> Optional[List[ResultBatch]]:
        if cursor is None:
            with self.cursor() as cur:
                cur.execute(
                    "alter session set python_connector_query_result_format='ARROW'"
                )
                cur.execute(query)
                batches = cur.get_result_batches()
        else:
            cursor.execute(
                "alter session set python_connector_query_result_format='ARROW'"
            )
            cursor.execute(query)
            batches = cursor.get_result_batches()
        return batches

    def query_to_arrow(
        self,
        query: str,
        schema: Any = None,
        infer_nested_types: bool = False,
        cursor: Optional[SnowflakeCursor] = None,
    ) -> pa.Table:
        return get_arrow_from_batches(
            self.query_to_result_batches(query, cursor=cursor),
            schema=schema,
            infer_nested_types=infer_nested_types,
        )

    def query_to_engine_df(
        self,
        query: str,
        engine: ExecutionEngine,
        schema: Any = None,
        infer_nested_types: bool = False,
        cursor: Optional[SnowflakeCursor] = None,
    ) -> DataFrame:
        if schema is not None:
            _schema = schema if isinstance(schema, Schema) else Schema(schema)
        else:
            tb = self.query_to_ibis(query)
            _schema = to_schema(tb.schema())

        batches = self.query_to_result_batches(query, cursor=cursor)

        if batches is None or len(batches) == 0:
            raise ValueError(f"No data returned from {query}")

        idx = ArrayDataFrame([[x] for x in range(len(batches))], "id:int")

        def _map(cursor: Any, df: LocalDataFrame) -> LocalDataFrame:
            _b = [batches[row["id"]] for row in df.as_dict_iterable()]  # type: ignore
            adf = get_arrow_from_batches(
                _b, schema=schema, infer_nested_types=infer_nested_types
            )
            return ArrowDataFrame(adf)

        res = engine.map_engine.map_dataframe(
            idx,
            _map,
            output_schema=_schema,
            partition_spec=PartitionSpec("per_row"),
        )
        return res

    def df_to_temp_table(
        self, df: DataFrame, engine: ExecutionEngine, transient: bool = True
    ) -> str:
        with _Uploader(
            self, self.sf.cursor(), self._database, self._schema
        ) as uploader:
            return uploader.to_temp_table(df, engine, transient=transient)

    def df_to_table(
        self,
        df: DataFrame,
        table: str,
        mode: str,
        engine: ExecutionEngine,
        table_type: str = "",
    ) -> str:
        with _Uploader(
            self, self.sf.cursor(), self._database, self._schema
        ) as uploader:
            return uploader.to_table(
                df, table, mode=mode, engine=engine, table_type=table_type
            )


class _Uploader:
    def __init__(
        self,
        client: SnowflakeClient,
        cursor: SnowflakeCursor,
        database: str,
        schema: str,
    ):
        self._client = client
        self._cursor = cursor
        self._database = database
        self._schema = schema
        self._stage = self._get_full_rand_name()

    def _get_full_rand_name(self) -> str:
        return self._database + "." + self._schema + "." + _temp_rand_str().upper()

    def __enter__(self) -> "_Uploader":
        create_stage_sql = (
            f"CREATE STAGE IF NOT EXISTS {self._stage}" " FILE_FORMAT=(TYPE=PARQUET)"
        )
        print(create_stage_sql)
        self._cursor.execute(create_stage_sql).fetchall()
        return self

    def __exit__(
        self, exception_type: Any, exception_value: Any, exception_traceback: Any
    ) -> None:
        drop_stage_sql = f"DROP STAGE IF EXISTS {self._stage}"
        print(drop_stage_sql)
        self._cursor.execute(drop_stage_sql).fetchall()

    def to_temp_table(
        self, df: DataFrame, engine: ExecutionEngine, transient: bool = False
    ) -> str:
        files = self.upload(df, engine)
        table = self._create_temp_table(df.schema, transient=transient)
        return self._copy_to_table(files, table)

    def to_table(
        self,
        df: DataFrame,
        table: str,
        mode: str,
        engine: ExecutionEngine,
        table_type: str = "",
    ) -> str:
        files = self.upload(df, engine)
        assert_or_throw(
            mode in ["overwrite", "append"], ValueError(f"Unsupported mode: {mode}")
        )
        if mode == "overwrite":
            self._cursor.execute(f"DROP TABLE IF EXISTS {table}").fetchall()
            table = self._create_table(df.schema, table, table_type=table_type)
        return self._copy_to_table(files, table)

    def upload(self, df: DataFrame, engine: ExecutionEngine) -> List[str]:
        stage_location = self._stage
        client = self._client

        def _map(cursor: Any, df: LocalDataFrame) -> LocalDataFrame:
            file = _temp_rand_str() + ".parquet"
            with TemporaryDirectory() as f:
                path = os.path.join(f, file)
                write_parquet(df.as_arrow(), path)
                with client.cursor() as cur:
                    cur.execute(f"PUT file://{path} @{stage_location}").fetchall()
            return ArrayDataFrame([[file]], "file:str")

        res = engine.map_engine.map_dataframe(
            df,
            _map,
            output_schema=Schema("file:str"),
            partition_spec=PartitionSpec(),
            map_func_format_hint="pyarrow",
        )
        return res.as_pandas().file.tolist()

    def _create_table(self, schema: Any, table: str, table_type: str) -> str:
        expr = to_snowflake_schema(schema)
        create_table_sql = f"CREATE {table_type.upper()} TABLE {table} ({expr})"
        print(create_table_sql)
        self._cursor.execute(create_table_sql)
        return table

    def _create_temp_table(self, schema: Any, transient: bool = False) -> str:
        table = self._get_full_rand_name()
        return self._create_table(schema, table, "TRANSIENT" if transient else "TEMP")

    def _copy_to_table(self, files: List[str], table: str) -> str:
        files_expr = ", ".join([f"'{x}'" for x in files])
        copy_sql = (
            f"COPY INTO {table} FROM"
            f" @{self._stage}"
            f" FILES = ({files_expr})"
            f" FILE_FORMAT = (TYPE=PARQUET)"
            f" MATCH_BY_COLUMN_NAME = CASE_SENSITIVE"
        )
        print(copy_sql)
        res = self._cursor.execute(copy_sql).fetchall()
        print(res)
        return table


def _temp_rand_str() -> str:
    return "temp_" + str(uuid4()).split("-")[0]


def _to_snowflake_select_schema(schema: Any) -> str:
    _s = schema if isinstance(schema, Schema) else Schema(schema)
    fields = []
    for f in _s.fields:
        fields.append(
            f"$1:{quote_name(f.name)}::{pa_type_to_snowflake_type_str(f.type)}"
        )
    return ", ".join(fields)
