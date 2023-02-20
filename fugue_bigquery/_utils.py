from typing import Any, List, Optional, Dict

import fugue.api as fa
import pyarrow as pa
from fugue_ibis import IbisTable, IbisSchema
from fugue_ibis._utils import ibis_to_pa_type, pa_to_ibis_type
from ibis.backends.bigquery import Backend
from triad import ParamDict, Schema, assert_or_throw
from triad.utils.pyarrow import TRIAD_DEFAULT_TIMESTAMP
from triad.utils.schema import quote_name
from typing import Iterable
from ._constants import (
    FUGUE_BQ_CONF_TEMP_DATASET,
    FUGUE_BQ_CONF_TEMP_DATASET_DEFAULT_NAME,
)
import ibis.expr.datatypes as dt
import logging
import re


def get_temp_dataset(conf: Optional[ParamDict] = None) -> str:
    if conf is not None and FUGUE_BQ_CONF_TEMP_DATASET in conf:
        return conf.get_or_throw(FUGUE_BQ_CONF_TEMP_DATASET, str)
    return fa.get_current_conf().get(
        FUGUE_BQ_CONF_TEMP_DATASET, FUGUE_BQ_CONF_TEMP_DATASET_DEFAULT_NAME
    )


def is_bq_ibis_table(df: Any):
    if not isinstance(df, IbisTable):
        return False
    try:
        return isinstance(df._find_backend(), Backend)
    except Exception:  # pragma: no cover
        return False


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


def normalize_pa_schema(schema: pa.Schema) -> pa.Schema:
    fields: List[Any] = []
    for f in schema:
        if _is_default_timestamp(f.type):
            fields.append(pa.field(f.name, TRIAD_DEFAULT_TIMESTAMP))
        else:
            fields.append(f)
    return pa.schema(fields)


def normalize_pa_table(tb: pa.Table) -> pa.Table:
    arr: List[Any] = []
    fields: List[Any] = []
    for f in tb.schema:
        if _is_default_timestamp(f.type):
            arr.append(tb[f.name].cast(TRIAD_DEFAULT_TIMESTAMP))
            fields.append(pa.field(f.name, TRIAD_DEFAULT_TIMESTAMP))
        else:
            arr.append(tb[f.name])
            fields.append(f)
    return pa.Table.from_arrays(arr, schema=pa.schema(fields))


def table_to_query(
    table: str,
    columns: Optional[List[str]] = None,
    row_filter: Optional[str] = None,
    sample: float = 1.0,
) -> str:
    def _build_sql() -> Iterable[str]:
        yield "SELECT"
        if columns is None or len(columns) == 0:
            yield "*"
        else:
            yield ",".join(quote_name(c) for c in columns)
        yield "FROM " + quote_name(table)
        if sample < 1.0:
            yield f"TABLESAMPLE SYSTEM ({sample*100} PERCENT)"
        if row_filter is not None and row_filter.strip() != "":
            yield "WHERE " + row_filter

    return " ".join(_build_sql())


def is_select_query(s: str) -> bool:
    return (
        re.match(r"^\s*select\s", s, re.IGNORECASE) is not None
        or re.match(r"^\s*with\s", s, re.IGNORECASE) is not None
    )


def _is_default_timestamp(tp: pa.DataType) -> bool:
    return pa.types.is_timestamp(tp) and str(tp.tz).lower() == "utc"
