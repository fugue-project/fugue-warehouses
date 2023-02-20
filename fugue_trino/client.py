import os
from contextvars import ContextVar
from typing import Any, List, Optional, Dict
from uuid import uuid4

import fugue.api as fa
import ibis
from fugue import AnyDataFrame, PandasDataFrame
from fugue_ibis._utils import to_ibis_schema
from ibis import BaseBackend
from triad import ParamDict, SerializableRLock, assert_or_throw
from trino.auth import BasicAuthentication
from trino.dbapi import connect
from trino.exceptions import HttpError

from ._constants import (
    FUGUE_TRINO_CONF_DATABASE,
    FUGUE_TRINO_CONF_HOST,
    FUGUE_TRINO_CONF_PASSWORD,
    FUGUE_TRINO_CONF_USER,
    FUGUE_TRINO_ENV_PASSWORD,
    FUGUE_TRINO_CONF_PORT,
)
from ._utils import get_temp_schema, to_schema

_FUGUE_TRINO_CLIENT_CONTEXT = ContextVar("_FUGUE_TRINO_CLIENT_CONTEXT", default=None)

_CONTEXT_LOCK = SerializableRLock()


class TrinoClient:
    @staticmethod
    def get_or_create(conf: Any = None) -> "TrinoClient":
        with _CONTEXT_LOCK:
            res = _FUGUE_TRINO_CLIENT_CONTEXT.get()
            if res is None:
                _conf = ParamDict(conf)
                tds = get_temp_schema(_conf)
                database = _conf.get_or_throw(FUGUE_TRINO_CONF_DATABASE, str)
                user = _conf.get_or_throw(FUGUE_TRINO_CONF_USER, str)
                host = _conf.get_or_throw(FUGUE_TRINO_CONF_HOST, str)
                password = _conf.get_or_none(FUGUE_TRINO_CONF_PASSWORD, str)
                port = _conf.get(FUGUE_TRINO_CONF_PORT, 443)
                res = TrinoClient(
                    temp_schema=tds,
                    database=database,
                    user=user,
                    host=host,
                    password=password,
                    port=port,
                )
                _FUGUE_TRINO_CLIENT_CONTEXT.set(res)  # type: ignore
            return res

    @staticmethod
    def get_current() -> "TrinoClient":
        with _CONTEXT_LOCK:
            res = _FUGUE_TRINO_CLIENT_CONTEXT.get()
            assert_or_throw(
                res is not None, ValueError("no TrinoClient was initialized")
            )
            return res  # type: ignore

    def __init__(
        self,
        temp_schema: str,
        database: str,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: int = 443,
        **connect_kwargs: Any,
    ):
        self._temp_schema = temp_schema
        self._temp_tables: List[str] = []
        self._database = database
        if password is None:
            password = os.environ.get(FUGUE_TRINO_ENV_PASSWORD, None)
        self._con_params = dict(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
            **connect_kwargs,
        )
        self._ibis = ibis.fugue_trino.connect(**self._con_params)
        self._trino_con = connect(
            host=host,
            port=port,
            auth=BasicAuthentication(user, password),
            http_scheme="https",
        )
        self._con_lock = SerializableRLock()
        self._schema_backends: Dict[str, BaseBackend] = {}

    @property
    def database(self) -> str:
        return self._database

    def table_to_full_name(self, name: str) -> str:
        s = name.split(".")
        if len(s) == 1:
            return self.database + "." + self._temp_schema + "." + name
        if len(s) == 2:
            return self.database + "." + name
        return name

    @property
    def ibis(self) -> BaseBackend:
        return self._ibis

    def stop(self):
        self.ibis.client.close()  # type: ignore

    def __enter__(self) -> "TrinoClient":
        return self

    def __exit__(
        self, exception_type: Any, exception_value: Any, exception_traceback: Any
    ) -> None:
        self.stop()

    def df_to_table(self, df: AnyDataFrame, table: Optional[str] = None) -> str:
        tb = (
            self.table_to_full_name(table)
            if table is not None
            else self._generate_temp_table()
        )
        fdf = fa.as_fugue_df(df)
        con = self.connect_to_schema(tb.split(".")[-2])
        try:
            con.create_table(
                tb.split(".")[-1], fdf.as_pandas(), schema=to_ibis_schema(fdf.schema)
            )
        except HttpError:
            pass
        return tb

    def connect_to_schema(self, schema: str) -> BaseBackend:
        with self._con_lock:
            if schema not in self._schema_backends:
                params = dict(self._con_params)
                params["schema"] = schema
                self._schema_backends[schema] = ibis.trino.connect(**params)
            return self._schema_backends[schema]

    def query_to_df(self, query: str) -> PandasDataFrame:
        t = self.ibis.sql(query)
        schema = to_schema(t.schema())
        return PandasDataFrame(t.execute(), schema=schema)

    def _generate_temp_table(self) -> str:
        name = self.table_to_full_name("temp_" + str(uuid4())[:7])
        self._temp_tables.append(name)
        return name
