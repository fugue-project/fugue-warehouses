from typing import Any

import fugue.test as ft
from fugue_test.execution_suite import ExecutionEngineTests
from triad import to_uuid
from fugue_snowflake import SnowflakeExecutionEngine, SnowflakeDataFrame


@ft.fugue_test_suite("snowflake", mark_test=True)
class SnowflakeExecutionEngineTestsBase(ExecutionEngineTests.Tests):
    def df(self, data: Any = None, schema: Any = None) -> SnowflakeDataFrame:
        if not hasattr(self, "_cache"):
            self._cache = {}
            self._data_ref = []
        if isinstance(data, list):
            key = to_uuid(data, schema)
        else:
            key = to_uuid(id(data), schema)
        if key in self._cache:
            return self._cache[key]
        res = self.engine.sql_engine.to_df(data, schema=schema)
        self._cache[key] = res
        self._data_ref.append(data)  # make sure the data is not removed by gc
        return res
