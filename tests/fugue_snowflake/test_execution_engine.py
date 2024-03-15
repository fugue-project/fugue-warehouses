from typing import Any

import fugue.test as ft
from fugue_test.execution_suite import ExecutionEngineTests
from triad import to_uuid

from fugue_snowflake import SnowflakeDataFrame, SnowflakeExecutionEngine


@ft.fugue_test_suite("snowflake", mark_test=True)
class SnowflakeExecutionEngineTestsBase(ExecutionEngineTests.Tests):
    pass
