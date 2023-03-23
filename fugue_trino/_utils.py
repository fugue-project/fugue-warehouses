import re
from typing import Any, List, Optional

import fugue.api as fa
import pyarrow as pa
from fugue.extensions import namespace_candidate
from fugue_ibis import IbisSchema, IbisTable
from fugue_ibis._utils import ibis_to_pa_type
from ibis.backends.trino import Backend
from triad import ParamDict, Schema
from triad.utils.pyarrow import TRIAD_DEFAULT_TIMESTAMP

from ._constants import (
    FUGUE_TRINO_CONF_TEMP_SCHEMA,
    FUGUE_TRINO_CONF_TEMP_SCHEMA_DEFAULT_NAME,
)


def get_temp_schema(conf: Optional[ParamDict] = None) -> str:
    if conf is not None and FUGUE_TRINO_CONF_TEMP_SCHEMA in conf:
        return conf.get_or_throw(FUGUE_TRINO_CONF_TEMP_SCHEMA, str)
    return fa.get_current_conf().get(
        FUGUE_TRINO_CONF_TEMP_SCHEMA, FUGUE_TRINO_CONF_TEMP_SCHEMA_DEFAULT_NAME
    )


is_trino_repr = namespace_candidate("trino", lambda x: isinstance(x, str))


def is_trino_ibis_table(df: Any):
    if not isinstance(df, IbisTable):
        return False
    try:
        return isinstance(df._find_backend(), Backend)
    except Exception:  # pragma: no cover
        return False


def to_schema(schema: IbisSchema) -> Schema:
    fields: List[pa.Field] = []
    for n, tp in zip(schema.names, schema.types):
        ptp = ibis_to_pa_type(tp)
        if _is_default_timestamp(ptp):
            ptp = TRIAD_DEFAULT_TIMESTAMP
        fields.append(pa.field(n, ptp))
    return Schema(fields)


def is_select_query(s: str) -> bool:
    return (
        re.match(r"^\s*select\s", s, re.IGNORECASE) is not None
        or re.match(r"^\s*with\s", s, re.IGNORECASE) is not None
    )


def _is_default_timestamp(tp: pa.DataType) -> bool:
    return pa.types.is_timestamp(tp) and str(tp.tz).lower() == "utc"
