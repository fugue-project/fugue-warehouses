from typing import Any, Optional

from fugue import DataFrame
from triad import to_uuid

from fugue_trino import TrinoClient, TrinoExecutionEngine
from fugue_trino._constants import FUGUE_TRINO_CONF_TEMP_SCHEMA_DEFAULT_NAME


def get_testing_client():
    client = TrinoClient.get_or_create(
        {
            "fugue.trino.catalog": "memory",
            "fugue.trino.user": "trino",
            "fugue.trino.host": "localhost",
            "fugue.trino.port": "8181",
        }
    )
    client.sql(
        "CREATE SCHEMA IF NOT EXISTS memory."
        f"{FUGUE_TRINO_CONF_TEMP_SCHEMA_DEFAULT_NAME}"
    )
    return client


class MockTrinoExecutionEngine(TrinoExecutionEngine):
    def __init__(self, client: Optional[TrinoClient] = None, conf: Any = None):
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
