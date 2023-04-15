import os
import warnings
from contextvars import ContextVar
from typing import Any, Dict, List, Optional
from uuid import uuid4

import fugue.api as fa
import ibis
import numpy as np
from fugue import AnyDataFrame
from fugue_ibis import IbisDataFrame, IbisTable
from fugue_ibis._utils import to_ibis_schema
from fugue_warehouses import WarehouseClientBase
from ibis import BaseBackend
from sqlalchemy import exc as sa_exc
from triad import ParamDict, SerializableRLock, assert_or_throw
from trino.auth import BasicAuthentication
from trino.dbapi import Connection as TrinoConnection
from trino.dbapi import Cursor as TrinoCursor
from trino.dbapi import connect
from trino.exceptions import HttpError

from ._constants import (
    FUGUE_TRINO_CONF_CATALOG,
    FUGUE_TRINO_CONF_HOST,
    FUGUE_TRINO_CONF_PASSWORD,
    FUGUE_TRINO_CONF_PORT,
    FUGUE_TRINO_CONF_USER,
    FUGUE_TRINO_ENV_PASSWORD,
)
from ._utils import get_temp_schema, is_trino_ibis_table, is_select_query
from .collections import TableName

_FUGUE_TRINO_CLIENT_CONTEXT = ContextVar("_FUGUE_TRINO_CLIENT_CONTEXT", default=None)

_CONTEXT_LOCK = SerializableRLock()


class TrinoClient(WarehouseClientBase):
    @staticmethod
    def get_or_create(conf: Any = None) -> "TrinoClient":
        with _CONTEXT_LOCK:
            res = _FUGUE_TRINO_CLIENT_CONTEXT.get()
            if res is None:
                _conf = ParamDict(conf)
                tds = get_temp_schema(_conf)
                catalog = _conf.get_or_throw(FUGUE_TRINO_CONF_CATALOG, str)
                user = _conf.get_or_throw(FUGUE_TRINO_CONF_USER, str)
                host = _conf.get_or_throw(FUGUE_TRINO_CONF_HOST, str)
                password = _conf.get_or_none(FUGUE_TRINO_CONF_PASSWORD, str)
                port = _conf.get(FUGUE_TRINO_CONF_PORT, 443)
                res = TrinoClient(
                    temp_schema=tds,
                    catalog=catalog,
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
        catalog: str,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: int = 443,
        **connect_kwargs: Any,
    ):
        self._temp_schema = temp_schema
        self._temp_tables: List[str] = []
        self._catalog = catalog
        if password is None:
            password = os.environ.get(FUGUE_TRINO_ENV_PASSWORD, None)
        ckw = ParamDict(connect_kwargs)
        ckw["catalog"] = catalog
        self._con_params = dict(
            user=user,
            password=password,
            host=host,
            port=port,
            **ckw,
        )
        self._ibis = ibis.fugue_trino.connect(**self._con_params)
        if password is not None:  # pragma: no cover
            self._trino_con = connect(
                host=host,
                port=port,
                auth=BasicAuthentication(user, password),
                http_scheme="https",
            )
        else:
            self._trino_con = connect(host=host, port=port, user=user)
        self._con_lock = SerializableRLock()
        self._schema_backends: Dict[str, BaseBackend] = {}

    @property
    def catalog(self) -> str:
        return self._catalog

    @property
    def connection(self) -> TrinoConnection:
        return self._trino_con

    def sql(self, query: str) -> TrinoCursor:
        cursor = self.connection.cursor()
        return cursor.execute(query)

    def to_table_name(self, name: Any) -> TableName:
        return TableName.parse(
            name,
            default_catalog=self.catalog,
            default_schema=self._temp_schema,
            table_func=self._generate_temp_table,
        )

    @property
    def ibis(self) -> BaseBackend:
        return self._ibis

    def stop(self):
        self.connection.close()
        self.ibis.con.dispose()

    def __enter__(self) -> "TrinoClient":
        return self

    def __exit__(
        self, exception_type: Any, exception_value: Any, exception_traceback: Any
    ) -> None:
        self.stop()

    def df_to_table(
        self, df: AnyDataFrame, table: Any = None, overwrite: bool = False
    ) -> TableName:
        tb = self.to_table_name(table)
        fdf = fa.as_fugue_df(df)
        con = self.connect_to_schema(tb.schema)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=sa_exc.SAWarning)
                if isinstance(fdf, IbisDataFrame) and is_trino_ibis_table(fdf.native):
                    obj: Any = fdf.native
                else:
                    obj = fdf.as_pandas().replace({np.nan: None})
                    if len(obj) == 0:
                        obj = None
                    else:
                        obj = ibis.memtable(obj, schema=to_ibis_schema(fdf.schema))
                con.create_table(
                    tb.table,
                    obj,
                    schema=to_ibis_schema(fdf.schema),
                    overwrite=overwrite,
                )
        except HttpError:  # pragma: no cover
            pass
        return tb

    def query_to_ibis(self, query: Any) -> IbisTable:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            if isinstance(query, str) and is_select_query(query):
                return self.ibis.sql(query)
            tb = self.to_table_name(query)
            return self.ibis.table(tb.table, schema=tb.schema)

    def connect_to_schema(self, schema: str) -> BaseBackend:
        with self._con_lock:
            if schema not in self._schema_backends:
                params = dict(self._con_params)
                params["schema"] = schema
                params["database"] = schema
                self._schema_backends[schema] = ibis.fugue_trino.connect(**params)
            return self._schema_backends[schema]

    def _generate_temp_table(self) -> str:
        return "temp_" + str(uuid4())[:7]
