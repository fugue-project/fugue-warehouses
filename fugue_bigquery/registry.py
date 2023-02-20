from typing import Any, Optional, Tuple

from fugue import DataFrame, ExecutionEngine, SQLEngine, is_pandas_or
from fugue.extensions import namespace_candidate
from fugue.plugins import (
    as_fugue_dataset,
    infer_execution_engine,
    parse_execution_engine,
    parse_sql_engine,
    parse_creator,
)
from triad import ParamDict

from ._utils import is_bq_ibis_table, is_select_query
from .api import load_sql, load_table
from .client import BigQueryClient
from .dataframe import BigQueryDataFrame
from .execution_engine import BigQueryExecutionEngine, BigQuerySQLEngine

_is_bigquery = namespace_candidate("bq", lambda x: isinstance(x, str))


@as_fugue_dataset.candidate(lambda df, **kwargs: _is_bigquery(df))
def _bq_to_df(query: Tuple[str, str], **kwargs: Any) -> DataFrame:
    if is_select_query(query[1]):
        return load_sql(query[1], as_fugue=True)
    else:
        return load_table(query[1], as_fugue=True)


@parse_creator.candidate(_is_bigquery)
def _parse_bq_creator(query: Tuple[str, str]):
    def _creator() -> DataFrame:
        return _bq_to_df(query)

    return _creator


@parse_execution_engine.candidate(
    matcher=lambda engine, conf, **kwargs: isinstance(engine, str)
    and (engine == "bq" or engine == "bigquery"),
    priority=2.5,
)
def _parse_bq(engine: str, conf: Any, **kwargs) -> ExecutionEngine:
    _conf = ParamDict(conf)
    client = BigQueryClient.get_or_create(_conf)
    return BigQueryExecutionEngine(client, _conf)


@infer_execution_engine.candidate(
    lambda objs: is_pandas_or(objs, BigQueryDataFrame)
    or any(
        is_bq_ibis_table(x) or (isinstance(x, str) and x == "force_bq") for x in objs
    )
)
def _infer_bq_engine(objs: Any) -> Any:
    return "bq"


@parse_sql_engine.candidate(
    lambda engine, *args, **kwargs: isinstance(engine, str)
    and engine in ["bq", "bigquery"]
)
def _to_bq_sql_engine(
    engine: str,
    execution_engine: Optional[ExecutionEngine] = None,
    **kwargs: Any,
) -> SQLEngine:
    return BigQuerySQLEngine(execution_engine)
