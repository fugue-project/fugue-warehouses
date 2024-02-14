from typing import Any, Optional, Tuple

from fugue import DataFrame, ExecutionEngine, SQLEngine, is_pandas_or
from fugue.extensions import namespace_candidate
from fugue.plugins import (
    as_fugue_dataset,
    infer_execution_engine,
    parse_creator,
    parse_execution_engine,
    parse_sql_engine,
)
from triad import ParamDict

from ._utils import is_sf_ibis_table
from .client import SnowflakeClient
from .dataframe import SnowflakeDataFrame
from .execution_engine import SnowflakeExecutionEngine, SnowflakeSQLEngine

_is_sf = namespace_candidate("sf", lambda x: isinstance(x, str))


@as_fugue_dataset.candidate(lambda df, **kwargs: _is_sf(df))
def _sf_to_df(query: Tuple[str, str], **kwargs: Any) -> DataFrame:
    client = SnowflakeClient.get_current()
    return SnowflakeDataFrame(client.query_or_table_to_ibis(query[1]))


@parse_creator.candidate(_is_sf)
def _parse_sf_creator(query: Tuple[str, str]):
    def _creator() -> DataFrame:
        return _sf_to_df(query)

    return _creator


@parse_execution_engine.candidate(
    matcher=lambda engine, conf, **kwargs: isinstance(engine, str)
    and (engine == "sf" or engine == "snowflake"),
    priority=2.5,
)
def _parse_sf(engine: str, conf: Any, **kwargs) -> ExecutionEngine:
    _conf = ParamDict(conf)
    client = SnowflakeClient.get_or_create(_conf)
    return SnowflakeExecutionEngine(client, _conf)


@infer_execution_engine.candidate(
    lambda objs: is_pandas_or(objs, SnowflakeDataFrame)
    or any(
        is_sf_ibis_table(x) or _is_sf(x) or (isinstance(x, str) and x == "force_sf")
        for x in objs
    )
)
def _infer_sf_engine(objs: Any) -> Any:
    print(objs)
    return "sf"


@parse_sql_engine.candidate(
    lambda engine, *args, **kwargs: isinstance(engine, str)
    and engine in ["sf", "snowflake"]
)
def _to_sf_sql_engine(
    engine: str,
    execution_engine: Optional[ExecutionEngine] = None,
    **kwargs: Any,
) -> SQLEngine:
    return SnowflakeSQLEngine(execution_engine)
