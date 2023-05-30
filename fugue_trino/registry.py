from typing import Any, Optional, Tuple

import fugue.plugins as fp
import fugue.api as fa
from fugue import DataFrame, ExecutionEngine, SQLEngine, is_pandas_or
from triad import ParamDict, Schema
from fugue_ibis import IbisTable
from ._utils import is_trino_ibis_table, is_trino_repr, to_schema
from .client import TrinoClient
from .dataframe import TrinoDataFrame
from .execution_engine import TrinoExecutionEngine, TrinoSQLEngine


@fp.as_fugue_dataset.candidate(lambda query, **kwargs: is_trino_repr(query))
def _trino_to_df(query: Tuple[str, str], **kwargs: Any) -> DataFrame:
    return TrinoDataFrame(TrinoClient.get_current().query_to_ibis(query[1]))


@fp.is_df.candidate(lambda df: is_trino_ibis_table(df) or is_trino_repr(df))
def _is_trino_df(df: Any):
    return True


@fp.get_schema.candidate(lambda df: is_trino_ibis_table(df) or is_trino_repr(df))
def _trino_get_schema(df: Any) -> Schema:
    """Get the schema of certain query or table

    :param query_or_table: the table name or query string
    :return: the schema of the output
    """
    if isinstance(df, IbisTable):
        return to_schema(df.schema())
    client = TrinoClient.get_or_create(fa.get_current_conf())
    return to_schema(client.query_to_ibis(df[1]).schema())


@fp.parse_execution_engine.candidate(
    matcher=lambda engine, conf, **kwargs: isinstance(engine, str)
    and engine == "trino",
    priority=2.5,
)
def _parse_trino(engine: str, conf: Any, **kwargs) -> ExecutionEngine:
    _conf = ParamDict(conf)
    client = TrinoClient.get_or_create(_conf)
    return TrinoExecutionEngine(client, _conf)


@fp.parse_execution_engine.candidate(
    lambda engine, conf, **kwargs: isinstance(engine, TrinoClient)
)
def _parse_trino_client(engine: TrinoClient, conf: Any, **kwargs) -> ExecutionEngine:
    return TrinoExecutionEngine(engine, conf)


@fp.infer_execution_engine.candidate(
    lambda objs: is_pandas_or(objs, TrinoDataFrame)
    or any(
        is_trino_ibis_table(x)
        or (isinstance(x, str) and x == "force_trino")
        or is_trino_repr(x)
        for x in objs
    )
)
def _infer_trino_engine(objs: Any) -> Any:
    return "trino"


@fp.parse_sql_engine.candidate(
    lambda engine, *args, **kwargs: isinstance(engine, str) and engine in ["trino"]
)
def _to_trino_sql_engine(
    engine: str,
    execution_engine: Optional[ExecutionEngine] = None,
    **kwargs: Any,
) -> SQLEngine:
    return TrinoSQLEngine(execution_engine)
