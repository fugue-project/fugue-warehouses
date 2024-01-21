from contextvars import ContextVar
from typing import Any, List, Optional
from uuid import uuid4

import ibis
import pyarrow
import pyarrow as pa
import snowflake.connector
from fugue import (
    AnyDataFrame,
    ArrayDataFrame,
    ArrowDataFrame,
    DataFrame,
    ExecutionEngine,
    LocalDataFrame,
    PartitionSpec,
)
from fugue_ibis import IbisTable
from triad import Schema, SerializableRLock, assert_or_throw

from ._constants import get_client_init_params
from ._utils import to_schema

_FUGUE_SNOWFLAKE_CLIENT_CONTEXT = ContextVar(
    "_FUGUE_SNOWFLAKE_CLIENT_CONTEXT", default=None
)

_CONTEXT_LOCK = SerializableRLock()


class SnowflakeClient:
    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        warehouse: Optional[str] = None,
        schema: Optional[str] = None,
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

    def query_to_arrow(self, query: str) -> pa.Table:
        with self.sf.cursor() as cur:
            cur.execute(
                "alter session set python_connector_query_result_format='ARROW'"
            )
            cur.execute(query)
            return cur.fetch_arrow_all()

    def query_to_engine_df(self, query: str, engine: ExecutionEngine) -> DataFrame:
        tb = self.query_to_ibis(query)
        schema = to_schema(tb.schema())

        with self.sf.cursor() as cur:
            cur.execute(
                "alter session set python_connector_query_result_format='ARROW'"
            )
            cur.execute(query)
            batches = cur.get_result_batches()

        if batches is None or len(batches) == 0:
            return ArrowDataFrame(schema=schema)

        idx = ArrayDataFrame([[x] for x in range(len(batches))], "id:int")

        def _map(cursor: Any, df: LocalDataFrame) -> LocalDataFrame:
            tbs: List[pa.Table] = []
            for row in df.as_dict_iterable():
                batch = batches[row["id"]]
                tbs.append(batch.to_arrow())
            res = pa.concat_tables(tbs)
            return ArrowDataFrame(res)

        res = engine.map_engine.map_dataframe(
            idx,
            _map,
            output_schema=schema,
            partition_spec=PartitionSpec("per_row"),
        )
        return res

    def load_df(self, df: DataFrame, name: str, mode: str = "overwrite") -> None:
        if isinstance(df, ArrayDataFrame):
            df_pandas = df.as_pandas()
        else:
            df_pandas = ArrowDataFrame(df).as_pandas()

        if mode == "overwrite":
            snowflake.connector.pandas_tools.write_pandas(
                self.sf, df_pandas, name, overwrite=True
            )
        elif mode == "append":
            snowflake.connector.pandas_tools.write_pandas(self.sf, df_pandas, name)
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    def create_temp_table(self, schema: Schema) -> str:
        temp_table_name = f"_temp_{uuid4().hex}"
        df = ArrayDataFrame(schema=schema)
        df_pandas = df.as_pandas()

        snowflake.connector.pandas_tools.write_pandas(
            self.sf, df_pandas, temp_table_name, overwrite=True, table_type="temporary"
        )

        self._temp_tables.append(temp_table_name)

        return temp_table_name

    def register_temp_table(self, name: str):
        self._temp_tables.append(name)

    def is_temp_table(self, name: str) -> bool:
        return name in self._temp_tables

    def df_to_table(
        self, df: AnyDataFrame, table_name: str = None, overwrite: bool = False
    ) -> Any:
        if table_name is None:
            if isinstance(df, ArrayDataFrame):
                schema = pyarrow.Table.from_pandas(df.as_pandas()).schema
            else:
                schema = ArrowDataFrame(df).schema
            table_name = self.create_temp_table(schema)

        self.load_df(df, table_name, mode="overwrite" if overwrite else "append")

        return table_name
