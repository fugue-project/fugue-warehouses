from typing import Any, List, Optional, Dict
import logging
import re

import pyarrow as pa
from fugue_ibis import IbisTable, IbisSchema
from fugue_ibis._utils import ibis_to_pa_type, pa_to_ibis_type
import ibis.expr.datatypes as dt
from triad import ParamDict, Schema, assert_or_throw
from triad.utils.pyarrow import TRIAD_DEFAULT_TIMESTAMP


def alter_table_columns(table: IbisTable, new_schema: Schema) -> IbisTable:
    fields: Dict[str, Any] = {}
    schema = table.schema()
    for _name, _type, f2 in zip(schema.names, schema.types, new_schema.fields):
        _new_name, _new_type = f2.name, pa_to_ibis_type(f2.type)
        assert_or_throw(
            _name == _new_name,
            lambda: ValueError(f"schema name mismatch: {_name} vs {_new_name}"),
        )
        if _type == _new_type:
            continue
        elif isinstance(_type, dt.GeoSpatial):
            fields[_name] = table[_name].as_text().cast(_new_type)
        else:
            fields[_name] = table[_name].cast(_new_type)
    if len(fields) == 0:
        return table
    return table.mutate(**fields)


def to_schema(schema: IbisSchema) -> Schema:
    fields: List[pa.Field] = []
    for n, tp in zip(schema.names, schema.types):
        if isinstance(tp, dt.GeoSpatial):
            logging.warning("%s:%s is not supported, converting to string", n, tp)
            fields.append(pa.field(n, pa.string()))
        # elif isinstance(tp, dt.Integer):
        #    fields.append(pa.field(n, pa.int64()))
        # elif isinstance(tp, dt.Floating):
        #    fields.append(pa.field(n, pa.float64()))
        else:
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
