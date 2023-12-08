from typing import Any, List, Optional

import fugue.api as fa
from fugue import AnyDataFrame, AnyExecutionEngine
from triad import Schema
from triad.utils.schema import quote_name

from ._utils import is_select_query
from .client import SnowflakeClient
from .dataframe import SnowflakeDataFrame

def get_schema(query_or_table: str) -> Schema:
    """Get the schema of certain query or table

    :param query_or_table: the table name or query string
    :return: the schema of the output
    """
    query = (
        query_or_table  # query
        if is_select_query(query_or_table)
        else f"SELECT * FROM {quote_name(query_or_table)}"
    )
    client = SnowflakeClient.get_or_create(fa.get_current_conf())
    tdf = SnowflakeDataFrame(client.ibis.sql(query))
    return tdf.schema