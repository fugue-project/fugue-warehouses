import base64
import os
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from tempfile import TemporaryDirectory
from typing import Any, Callable, Iterable, Iterator, List, Optional
import cloudpickle
import ibis
import pandas as pd
import pyarrow as pa
import snowflake.connector
from fugue import (
    ArrayDataFrame,
    ArrowDataFrame,
    DataFrame,
    ExecutionEngine,
    LocalDataFrame,
    LocalDataFrameIterableDataFrame,
    NativeExecutionEngine,
    PandasDataFrame,
    PartitionCursor,
    PartitionSpec,
)
from fugue_ibis import IbisTable
from pyarrow.parquet import write_table as write_parquet
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.result_batch import ResultBatch
from triad import Schema, SerializableRLock, assert_or_throw

from fugue_snowflake._utils import temp_rand_str

from ._constants import get_client_init_params
from ._utils import (
    build_package_list,
    get_arrow_from_batches,
    pa_type_to_snowflake_type_str,
    parse_table_name,
    quote_name,
    to_schema,
    to_snowflake_schema,
    unquote_name,
    is_select_query,
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

        self._parallelism = 0

        self._ibis = ibis.snowflake.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=f"{database}/{schema}",
            role=role,
        )

        con = self._ibis.con.connect()
        self._dialect = con.dialect
        self._compiler = self._ibis.compiler
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
        self._ibis = ibis.fugue_snowflake.connect(
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

    def get_current_parallelism(self) -> int:
        if self._parallelism == 0:
            try:
                warehouse = self.command_to_pandas("SELECT CURRENT_WAREHOUSE()").iloc[
                    0, 0
                ]
                all_warehouses = self.command_to_pandas("SHOW WAREHOUSES")
                size = (
                    all_warehouses[all_warehouses["name"] == warehouse]["size"]
                    .iloc[0]
                    .lower()
                )
                if size == "x-small":
                    return 1
                elif size == "small":
                    return 2
                elif size == "medium":
                    return 4
                elif size == "large":
                    return 8
                elif size == "x-large":
                    return 16
                elif "x-large" in size:
                    return 16 * int(size.split("x-large")[0])
                raise NotImplementedError(f"Unknown warehouse size: {size}")
            except Exception:
                raise
                self._parallelism = 1
        return self._parallelism

    def table_to_full_name(self, table: str) -> str:
        parts = parse_table_name(table)
        if len(parts) == 1:
            return f"{self._database}.{self._schema}.{parts[0]}"
        if len(parts) == 2:
            return f"{self._database}.{parts[0]}.{parts[1]}"
        if len(parts) == 3:
            return f"{parts[0]}.{parts[1]}.{parts[2]}"
        raise ValueError(f"Invalid table name: {table}")

    def table_exists(self, table: str) -> bool:
        full_name = self.table_to_full_name(table)
        parts = parse_table_name(full_name)
        tb = unquote_name(parts[-1]).replace("'", "\\'")
        sql = f"""
        SHOW TERSE TABLES LIKE '{tb}'
        IN {parts[0]}.{parts[1]}
        LIMIT 1"""
        print(sql)
        with self.cursor() as cur:
            res = cur.execute(sql).fetchall()
            return len(res) > 0

    def ibis_to_temp_table(self, table: IbisTable) -> str:
        full_name = self.table_to_full_name(temp_rand_str())
        qot = self.ibis_to_query_or_table(table, force_query=True)
        with self.cursor() as cur:
            cur.execute(f"CREATE TEMP TABLE {full_name} AS {qot}").fetchall()
        return full_name

    def ibis_to_table(self, table: IbisTable, name: str, mode: str) -> str:
        full_name = self.table_to_full_name(name)
        query = self.ibis_to_query_or_table(table, force_query=True)
        with self.cursor() as cur:
            if mode == "overwrite":
                cur.execute(f"DROP TABLE IF EXISTS {full_name}")
                cur.execute(f"CREATE OR REPLACE TABLE {full_name} AS {query}")
            elif mode == "append":
                if not self.table_exists(name):
                    cur.execute(f"CREATE TABLE {full_name} AS {query}")
                else:
                    cur.execute(f"INSERT INTO {full_name} {query}")
            else:
                raise NotImplementedError(f"Unsupported mode: {mode}")
        return full_name

    def ibis_to_query_or_table(
        self, table: IbisTable, force_query: bool = False
    ) -> str:
        if table.has_name() and not force_query:
            return table.get_name()
        query_ast = self._compiler.to_ast_ensure_limit(table)
        res = str(
            query_ast.compile().compile(
                dialect=self._dialect, compile_kwargs={"literal_binds": True}
            )
        )
        print("ibis_to_query", force_query, res)
        return res

    def query_or_table_to_ibis(self, query_or_table: str) -> IbisTable:
        if is_select_query(query_or_table):
            full_name = self.table_to_full_name("VIEW_" + temp_rand_str())
            with self.cursor() as cur:
                view_sql = f"CREATE TEMP VIEW {full_name} AS {query_or_table}"
                print(view_sql)
                cur.execute(view_sql).fetchall()
        else:
            full_name = self.table_to_full_name(query_or_table)
        parts = parse_table_name(full_name, normalize=True)
        return self.ibis.table(
            unquote_name(parts[2]),
            database=unquote_name(parts[0]),
            schema=unquote_name(parts[1]),
        )

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

    def ibis_to_arrow(
        self,
        table: IbisTable,
        schema: Any = None,
        infer_nested_types: bool = False,
        cursor: Optional[SnowflakeCursor] = None,
    ) -> pa.Table:
        query = self.ibis_to_query_or_table(table, force_query=True)
        return self.query_to_arrow(
            query, schema=schema, infer_nested_types=infer_nested_types, cursor=cursor
        )

    def query_to_engine_df(
        self,
        query: str,
        engine: ExecutionEngine,
        schema: Any = None,
        infer_nested_types: bool = False,
        cursor: Optional[SnowflakeCursor] = None,
    ) -> DataFrame:
        if _is_snowflake_engine(engine):
            return engine.to_df(self.query_or_table_to_ibis(query), schema=schema)
        if schema is not None:
            _schema = schema if isinstance(schema, Schema) else Schema(schema)
        else:
            tb = self.query_or_table_to_ibis(query)
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
        self, df: DataFrame, engine: ExecutionEngine, transient: bool = False
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
        if _is_snowflake_engine(engine):
            _df = engine.to_df(df)
            self.ibis_to_table(_df.native, table, mode=mode)
        with _Uploader(
            self, self.sf.cursor(), self._database, self._schema
        ) as uploader:
            return uploader.to_table(
                df,
                self.table_to_full_name(table),
                mode=mode,
                engine=engine,
                table_type=table_type,
            )

    def command_to_pandas(self, command: str) -> pd.DataFrame:
        with self.cursor() as cur:
            data = cur.execute(command).fetchall()
            columns = [x[0] for x in cur.description]
            return pd.DataFrame(data, columns=columns)

    def register_udtf(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]],
        additional_packages: str,
    ) -> str:
        schema = df.schema
        output_schema = Schema(output_schema)
        cursor = partition_spec.get_cursor(schema, 0)
        self.additional_packages = additional_packages

        def _run(pdf: pd.DataFrame) -> Iterable[pd.DataFrame]:  # pragma: no cover
            # the function must be defined here, so that it can be pickled
            # without unnecessary dependencies
            if pdf.shape[0] == 0:
                yield output_schema.create_empty_pandas_df()
                return
            input_df = PandasDataFrame(pdf, schema, pandas_df_wrapper=True)
            if on_init is not None:
                on_init(0, input_df)
            cursor.set(lambda: input_df.peek_array(), 0, 0)
            output_df = map_func(cursor, input_df)
            if isinstance(output_df, LocalDataFrameIterableDataFrame):
                for res in output_df.native:
                    yield res.as_pandas()
            else:
                yield output_df.as_pandas()

        udtf_name = "FUGUE_" + temp_rand_str()
        blob = base64.b64encode(cloudpickle.dumps(_run)).decode("ascii")
        _input_schema = to_snowflake_schema(schema)
        _output_schema = to_snowflake_schema(output_schema)
        pv = sys.version_info
        python_version = f"{pv.major}.{pv.minor}"
        packages = ["pandas", "cloudpickle", "fugue==0.8.7"]
        if self.additional_packages != "":
            packages.extend(
                x.strip().replace(" ", "") for x in self.additional_packages.split(",")
            )
        package_list = str(tuple(build_package_list(packages)))
        udtf_create = f"""
CREATE OR REPLACE TEMP FUNCTION {udtf_name}({_input_schema})
RETURNS TABLE ({_output_schema})
LANGUAGE PYTHON
RUNTIME_VERSION={python_version}
PACKAGES={package_list}
--IMPORTS=('@fugue_staging/fugue-warehouses.zip')
HANDLER='FugueTransformer'
AS $$
from _snowflake import vectorized
import pandas as pd
import cloudpickle
import base64

class FugueTransformer:
    @vectorized(input=pd.DataFrame)
    def end_partition(self, df):
        blob = '{blob}'
        func = cloudpickle.loads(base64.b64decode(blob.encode("ascii")))
        yield from func(df)
$$;"""
        print(udtf_create)
        with self.cursor() as cur:
            # cur.execute("CREATE TEMP STAGE IF NOT EXISTS fugue_staging;")
            # cur.execute("PUT file:///tmp/fugue-warehouses.zip @fugue_staging/")
            cur.execute(udtf_create)
        return udtf_name


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
        return self._database + "." + self._schema + "." + temp_rand_str().upper()

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
        if _is_snowflake_engine(engine):
            _engine = NativeExecutionEngine()
        else:
            _engine = engine
        stage_location = self._stage
        client = self._client

        def _map(cursor: Any, df: LocalDataFrame) -> LocalDataFrame:
            file = temp_rand_str() + ".parquet"
            with TemporaryDirectory() as f:
                path = os.path.join(f, file)
                write_parquet(df.as_arrow(), path)
                with client.cursor() as cur:
                    cur.execute(f"PUT file://{path} @{stage_location}").fetchall()
            return ArrayDataFrame([[file]], "file:str")

        res = _engine.map_engine.map_dataframe(
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


def _to_snowflake_select_schema(schema: Any) -> str:
    _s = schema if isinstance(schema, Schema) else Schema(schema)
    fields = []
    for f in _s.fields:
        fields.append(
            f"$1:{quote_name(f.name)}::{pa_type_to_snowflake_type_str(f.type)}"
        )
    return ", ".join(fields)


def _is_snowflake_engine(engine: ExecutionEngine) -> bool:
    return hasattr(engine, "_is_snowflake_engine") and engine._is_snowflake_engine
