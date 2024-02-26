from contextlib import contextmanager
from typing import Any, Dict, Iterator

import fugue.test as ft

from .client import SnowflakeClient


@ft.fugue_test_backend
class SnowflakeTestBackend(ft.FugueTestBackend):
    name = "snowflake"
    default_fugue_conf: Dict[str, Any] = {}

    @classmethod
    @contextmanager
    def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
        yield SnowflakeClient.get_or_create(session_conf)
