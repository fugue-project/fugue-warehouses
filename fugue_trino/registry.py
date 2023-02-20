from typing import Any

from fugue import ExecutionEngine, is_pandas_or
from fugue.plugins import infer_execution_engine, parse_execution_engine
from triad import ParamDict

from ._utils import is_trino_ibis_table
from .client import TrinoClient
from .dataframe import TrinoDataFrame
from .execution_engine import TrinoExecutionEngine


@parse_execution_engine.candidate(
    matcher=lambda engine, conf, **kwargs: isinstance(engine, str)
    and engine == "trino",
    priority=2.5,
)
def _parse_trino(engine: str, conf: Any, **kwargs) -> ExecutionEngine:
    _conf = ParamDict(conf)
    client = TrinoClient.get_or_create(_conf)
    return TrinoExecutionEngine(client, _conf)


@infer_execution_engine.candidate(
    lambda objs: is_pandas_or(objs, TrinoDataFrame)
    or any(
        is_trino_ibis_table(x) or (isinstance(x, str) and x == "force_trino")
        for x in objs
    )
)
def _infer_trino_engine(objs: Any) -> Any:
    return "trino"
