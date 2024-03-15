import pytest
import fugue.plugins as fp
from fugue_snowflake import SnowflakeExecutionEngine
from typing import Dict, Any, List
from triad import to_uuid

_CACHE: Dict[str, Any] = {}
_CACHE_REF: List[Any] = []


@fp.as_fugue_engine_df.candidate(
    lambda engine, *args, **kwargs: isinstance(engine, SnowflakeExecutionEngine)
)
def _sf_engine_to_df(engine: SnowflakeExecutionEngine, df, schema=None):
    if isinstance(df, list):
        key = to_uuid(df, schema)
    else:
        key = to_uuid(id(df), schema)
    if key in _CACHE:
        return _CACHE[key]
    res = engine.sql_engine.to_df(df, schema=schema)
    _CACHE[key] = res
    _CACHE_REF.append(df)  # make sure the data is not removed by gc
    return res


@pytest.fixture(scope="module")
def snowflake_session():
    from fugue_snowflake.tester import SnowflakeTestBackend

    with SnowflakeTestBackend.generate_session_fixture() as session:
        yield session
