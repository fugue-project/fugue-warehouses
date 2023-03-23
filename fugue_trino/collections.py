from dataclasses import dataclass
from typing import Any, Callable, Optional, List


@dataclass
class TableName:
    """Class for keeping track of an item in inventory."""

    catalog: str
    schema: str
    table: str

    def __str__(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

    @staticmethod
    def parse(  # noqa: C901
        obj: Any,
        default_catalog: Optional[str] = None,
        default_schema: Optional[str] = None,
        table_func: Optional[Callable[[], str]] = None,
    ) -> "TableName":
        if isinstance(obj, TableName):
            return obj
        if obj is None:
            obj = ""
        if isinstance(obj, str):
            parts: List[Any] = [x.strip() for x in obj.strip().split(".")]
        elif isinstance(obj, (tuple, list)):
            parts = [x if x is None else str(x) for x in obj]
        else:
            raise ValueError(f"{obj} can't be parsed into a table name")
        if len(parts) == 1:
            res: List[Optional[str]] = [default_catalog, default_schema, parts[0]]
        elif len(parts) == 2:
            res = [default_catalog, parts[0], parts[1]]
        elif len(parts) == 3:
            res = [parts[0], parts[1], parts[2]]
        else:
            raise ValueError(f"{obj} can't be parsed into a table name")
        if res[2] == "" and table_func is not None:
            res[2] = table_func()
        if res[0] is None or res[0] == "":
            raise ValueError("catalog is not provided")
        if res[1] is None or res[1] == "":
            raise ValueError("schema is not provided")
        if res[2] is None or res[2] == "":
            raise ValueError("table name is not provided")
        return TableName(catalog=res[0], schema=res[1], table=res[2])
