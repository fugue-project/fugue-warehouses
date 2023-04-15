from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

class WarehouseClientBase(ABC):
    def __init__(self):
        self._temp_tables: List[str] = []

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def __enter__(self) -> "WarehouseClientBase":
        pass

    @abstractmethod
    def __exit__(
        self, exception_type: Any, exception_value: Any, exception_traceback: Any
    ) -> None:
        pass

    @abstractmethod
    def connect_to_schema(self, schema: str) -> Any:
        pass

    @property
    @abstractmethod
    def ibis(self) -> Any:
        pass

    @abstractmethod
    def query_to_ibis(self, query: Any) -> Any:
        pass

    @abstractmethod
    def df_to_table(
        self, df: AnyDataFrame, table: Any = None, overwrite: bool = False
    ) -> Any:
        pass