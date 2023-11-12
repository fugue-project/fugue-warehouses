import json
import os
from contextvars import ContextVar
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

import ibis
import pyarrow
import snowflake.connector
from snowflake.connector.constants import FIELD_TYPES
from fugue import (
    ArrayDataFrame,
    ArrowDataFrame,
    DataFrame,
    LocalDataFrame,
    PartitionSpec,
    AnyDataFrame,
)
from fugue_ibis import IbisTable
from triad import ParamDict, Schema, SerializableRLock, assert_or_throw

from ._constants import (
    FUGUE_SNOWFLAKE_CONF_CREDENTIALS_ENV,
    FUGUE_SNOWFLAKE_CONF_ACCOUNT,
)

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
        schema: Optional[str] = None,
        credentials_func: Optional[Callable[[], Dict[str, Any]]] = None,
    ):
        self._temp_tables: List[str] = []
        self._account = account
        self._user = user
        self._password = password
        self._database = database
        self._schema = schema

        self._sf = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
        )

        self._ibis = ibis.snowflake.connect(
            account=account,
            user=user,
            password=password,
            database=f"{database}/{schema}",
        )

    @staticmethod
    def get_or_create(conf: Any = None) -> "SnowflakeClient":
        with _CONTEXT_LOCK:
            res = _FUGUE_SNOWFLAKE_CLIENT_CONTEXT.get()
            if res is None:
                _conf = ParamDict(conf)
                account = _conf.get_or_none(FUGUE_SNOWFLAKE_CONF_ACCOUNT, str)
                ce = _conf.get_or_none(FUGUE_SNOWFLAKE_CONF_CREDENTIALS_ENV, str)
                if ce is not None:
                    info = json.loads(os.environ[ce])
                    credentials_func: Any = lambda: info
                else:
                    credentials_func = None
                res = SnowflakeClient(
                    account=account, credentials_func=credentials_func
                )
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
        for tt in self._temp_tables:
            self._sf.cursor().execute(f"DROP TABLE IF EXISTS {tt}")
        self._sf.close()

    def __enter__(self) -> "SnowflakeClient":
        return self

    def __exit__(
        self, exception_type: Any, exception_value: Any, exception_traceback: Any
    ) -> None:
        self.stop()

    def connect_to_schema(self, schema: str) -> Any:
        self._sf.cursor().execute(f"USE SCHEMA {schema}")

    @property
    def ibis(self) -> ibis.BaseBackend:
        return self._ibis

    def query_to_ibis(self, query: str) -> IbisTable:
        return IbisTable(self.ibis.sql(query))

    def execute_to_df(
        self, query: str, columns: Optional[Schema] = None
    ) -> LocalDataFrame:
        cursor = self._sf.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        if columns is None:
            cols = cursor.description
            pa_schema = pyarrow.schema([(c[0], FIELD_TYPES[c[1]].pa_type()) for c in cols])
            columns = Schema(pa_schema)
        return ArrayDataFrame(rows, columns)

    def load_df(
        self,
        df: DataFrame,
        name: str,
        mode: str = "overwrite"
    ) -> None:
        if isinstance(df, ArrayDataFrame):
            df_pandas = df.as_pandas()
        else:
            df_pandas = ArrowDataFrame(df).as_pandas()

        if mode == "overwrite":
            snowflake.connector.pandas_tools.write_pandas(self.sf, df_pandas, name, overwrite=True)
        elif mode == "append":
            snowflake.connector.pandas_tools.write_pandas(self.sf, df_pandas, name)
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    def save_df(
        self,
        path: str,
        format_hint: Any = None,
        partition_spec: PartitionSpec = None,
        mode: str = "overwrite",
        **kwargs: Any,
    ) -> Callable[[DataFrame], None]:
        def _save(df: DataFrame) -> None:
            self.load_df(df, path, format_hint=format_hint, mode=mode, **kwargs)

        return _save

    def create_temp_table(self, schema: Schema) -> str:
        temp_table_name = f"_temp_{uuid4().hex}"
        df = ArrayDataFrame(schema=schema)
        df_pandas = df.as_pandas()

        snowflake.connector.pandas_tools.write_pandas(
            self.sf, 
            df_pandas, 
            temp_table_name, 
            overwrite=True, 
            table_type="temporary"
        )

        self._temp_tables.append(temp_table_name)

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

