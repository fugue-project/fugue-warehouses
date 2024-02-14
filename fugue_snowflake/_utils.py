import re
from importlib.metadata import version as get_version
from typing import Any, Dict, Iterable, List, Optional, Set
from uuid import uuid4

import pyarrow as pa
from fugue_ibis import IbisSchema, IbisTable
from fugue_ibis._utils import to_schema as _to_schema
from ibis.backends.snowflake import Backend
from snowflake.connector.constants import FIELD_TYPES
from snowflake.connector.result_batch import ResultBatch
from triad import Schema
from triad.utils.pyarrow import (
    get_alter_func,
    parse_json_columns,
    replace_types_in_table,
)

_PA_TYPE_TO_SF_TYPE: Dict[pa.DataType, str] = {
    pa.string(): "STRING",
    pa.bool_(): "BOOLEAN",
    pa.int8(): "BYTEINT",
    pa.int16(): "TINYINT",
    pa.int32(): "SMALLINT",
    pa.int64(): "BIGINT",
    pa.uint8(): "INT",
    pa.uint16(): "INT",
    pa.uint32(): "INT",
    pa.uint64(): "INT",
    pa.float16(): "FLOAT",
    pa.float32(): "FLOAT",
    pa.float64(): "FLOAT",
    pa.date32(): "DATE",
    pa.binary(): "BINARY",
}


def quote_name(name: str) -> str:
    quote = '"'
    return quote + name.replace(quote, quote + quote) + quote


def unquote_name(name: str) -> str:
    name = (
        name.replace('""', "<DOUBLE_QUOTE>")
        .replace('"', "")
        .replace("<DOUBLE_QUOTE>", '"')
    )
    return name


def normalize_name(name: str) -> str:
    if name.startswith('"') and name.endswith('"'):
        return name
    return name.upper()


def parse_table_name(name: str, normalize: bool = False) -> List[str]:
    res: List[str] = []
    start, p = 0, 0
    while p < len(name):
        if name[p] == '"':
            p += 1
            while p < len(name):
                if name[p] == '"':
                    if p + 1 < len(name) and name[p + 1] == '"':
                        p += 1
                    else:
                        break
                p += 1
            p += 1
        elif name[p] == ".":
            res.append(name[start:p])
            start = p + 1
            p += 1
        else:
            p += 1
    if start < len(name):
        res.append(name[start:])
    if normalize:
        return [normalize_name(x) for x in res]
    return res


def to_schema(schema: IbisSchema) -> Schema:
    return _to_schema(schema)


def pa_type_to_snowflake_type_str(tp: pa.DataType) -> str:
    if tp in _PA_TYPE_TO_SF_TYPE:
        return _PA_TYPE_TO_SF_TYPE[tp]
    if pa.types.is_timestamp(tp):
        if tp.tz is not None:
            return "TIMESTAMP_TZ"
        return "TIMESTAMP_NTZ"
    if pa.types.is_decimal(tp):
        return f"DECIMAL({tp.precision},{tp.scale})"
    if pa.types.is_list(tp):
        # itp = pa_type_to_snowflake_type_str(tp.value_type)
        # return f"ARRAY({itp})"
        return "ARRAY"
    if pa.types.is_struct(tp):
        # fields = []
        # for f in tp:
        #     fields.append(
        #         f"{quote_name(f.name)} {pa_type_to_snowflake_type_str(f.type)}"
        #     )
        # return f"OBJECT({', '.join(fields)})"
        return "OBJECT"
    if pa.types.is_map(tp):
        # ktp = pa_type_to_snowflake_type_str(tp.key_type)
        # vtp = pa_type_to_snowflake_type_str(tp.item_type)
        # return f"MAP({ktp}, {vtp})"
        return "MAP"
    raise NotImplementedError(f"Unsupported type {tp}")


def fix_snowflake_arrow_result(result: pa.Table) -> pa.Table:
    return replace_types_in_table(
        result,
        [
            (lambda tp: pa.types.is_integer(tp), pa.int64()),
            (lambda tp: pa.types.is_floating(tp), pa.float64()),
            (
                lambda tp: pa.types.is_decimal(tp)
                and tp.precision == 38
                and tp.scale == 0,
                pa.int64(),
            ),
            (lambda tp: pa.types.is_date64(tp), pa.date32()),
            # (
            #     lambda tp: pa.types.is_timestamp(tp)
            #     and tp.tz is None
            #     and tp != TRIAD_DEFAULT_TIMESTAMP,
            #     TRIAD_DEFAULT_TIMESTAMP,
            # ),
        ],
    )


def is_sf_ibis_table(df: Any):
    if not isinstance(df, IbisTable):
        return False
    try:
        return isinstance(df._find_backend(), Backend)
    except Exception:  # pragma: no cover
        return False


def to_snowflake_schema(schema: Any) -> str:
    _s = schema if isinstance(schema, Schema) else Schema(schema)
    fields = []
    for f in _s.fields:
        fields.append(f"{quote_name(f.name)} {pa_type_to_snowflake_type_str(f.type)}")
    return ", ".join(fields)


def get_arrow_from_batches(
    batches: Optional[List[ResultBatch]],
    query_output_schema: Schema,
    schema: Any = None,
    infer_nested_types: bool = False,
) -> pa.Table:
    if batches is None or len(batches) == 0:
        if schema is not None:
            return (
                schema if isinstance(schema, Schema) else Schema(schema)
            ).create_empty_arrow_table()
        return query_output_schema.create_empty_arrow_table()

    def _batches_to_arrow(_batches: List[ResultBatch]) -> Iterable[pa.Table]:
        has_result = False
        for batch in _batches:
            adf = batch.to_arrow()
            if adf.num_rows == 0:
                continue
            func = get_alter_func(adf.schema, query_output_schema.pa_schema, safe=True)
            has_result = True
            yield func(adf)

        if not has_result:
            yield query_output_schema.create_empty_arrow_table()

    adf = pa.concat_tables(_batches_to_arrow(batches))

    nested_cols = _get_nested_columns(batches[0])
    if infer_nested_types and len(nested_cols) > 0:
        adf = parse_json_columns(adf, nested_cols)
    if schema is not None:
        _schema = schema if isinstance(schema, Schema) else Schema(schema)
        func = get_alter_func(adf.schema, _schema.pa_schema, safe=True)
        adf = func(adf)
    return adf


def _get_nested_columns(batch: ResultBatch) -> List[str]:
    res: List[str] = []
    for meta in batch.schema:
        f = FIELD_TYPES[meta.type_code]
        if f.name in ["OBJECT", "ARRAY", "MAP", "VARIANT"]:
            res.append(meta.name)
    return res


def _get_batch_arrow_schema(batch: ResultBatch) -> pa.Schema:
    fields = [
        pa.field(s.name, FIELD_TYPES[s.type_code].pa_type()) for s in batch.schema
    ]
    return pa.schema(fields)


def temp_rand_str() -> str:
    return ("temp_" + str(uuid4()).split("-")[0]).upper()


def build_package_list(packages: Iterable[str]) -> List[str]:
    ps: Set[str] = set()
    for p in packages:
        ps.add(p)
        continue
        try:
            if "=" in p or "<" in p or ">" in p:
                ps.add(p)
            else:
                ps.add(p + "==" + get_version(p))
        except Exception:  # pragma: no cover
            ps.add(p)
    return list(ps)


def is_select_query(s: str) -> bool:
    return (
        re.match(r"^\s*select\s", s, re.IGNORECASE) is not None
        or re.match(r"^\s*with\s", s, re.IGNORECASE) is not None
    )
