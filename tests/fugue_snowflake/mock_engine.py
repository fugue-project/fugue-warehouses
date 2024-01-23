from typing import Any, Optional

from fugue import DataFrame
from triad import to_uuid

from fugue_snowflake import SnowflakeClient, SnowflakeExecutionEngine


class MockSnowflakeExecutionEngine(SnowflakeExecutionEngine):
    def __init__(self, client: Optional[SnowflakeClient] = None, conf: Any = None):
        super().__init__(client, conf)
        self._cache = {}

    def to_df(self, df: Any, schema: Any = None) -> DataFrame:
        if isinstance(df, list):
            key = to_uuid(df, schema)
        else:
            key = to_uuid(id(df), schema)
        if key in self._cache:
            return self._cache[key]
        res = super().sql_engine.to_df(df, schema)
        self._cache[key] = res
        return res
