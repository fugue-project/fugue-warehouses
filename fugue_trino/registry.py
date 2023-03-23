from typing import Any, Optional, Tuple

import fugue.plugins as fp
from fugue import DataFrame, ExecutionEngine, SQLEngine, is_pandas_or
from triad import ParamDict

from ._utils import is_trino_ibis_table, is_trino_repr
from .client import TrinoClient
from .dataframe import TrinoDataFrame
from .execution_engine import TrinoExecutionEngine, TrinoSQLEngine


@fp.as_fugue_dataset.candidate(lambda query, **kwargs: is_trino_repr(query))
def _trino_to_df(query: Tuple[str, str], **kwargs: Any) -> DataFrame:
    return TrinoDataFrame(TrinoClient.get_current().query_to_ibis(query[1]))


@fp.parse_creator.candidate(is_trino_repr)
def _parse_trino_creator(query: Tuple[str, str]):
    def _creator() -> DataFrame:
        return _trino_to_df(query)

    return _creator


@fp.parse_execution_engine.candidate(
    matcher=lambda engine, conf, **kwargs: isinstance(engine, str)
    and engine == "trino",
    priority=2.5,
)
def _parse_trino(engine: str, conf: Any, **kwargs) -> ExecutionEngine:
    _conf = ParamDict(conf)
    client = TrinoClient.get_or_create(_conf)
    return TrinoExecutionEngine(client, _conf)


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
