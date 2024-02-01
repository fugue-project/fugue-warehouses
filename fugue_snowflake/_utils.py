from typing import Any, Dict, List, Optional

import pyarrow as pa
from fugue_ibis import IbisSchema
from fugue_ibis._utils import to_schema as _to_schema
from snowflake.connector.constants import FIELD_TYPES
from snowflake.connector.result_batch import ResultBatch
from triad import Schema
from triad.utils.pyarrow import (
    TRIAD_DEFAULT_TIMESTAMP,
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
            (lambda tp: pa.types.is_date64(tp), pa.date32()),
            (
                lambda tp: pa.types.is_timestamp(tp)
                and tp.tz is None
                and tp != TRIAD_DEFAULT_TIMESTAMP,
                TRIAD_DEFAULT_TIMESTAMP,
            ),
        ],
    )


def to_snowflake_schema(schema: Any) -> str:
    _s = schema if isinstance(schema, Schema) else Schema(schema)
    fields = []
    for f in _s.fields:
        fields.append(f"{quote_name(f.name)} {pa_type_to_snowflake_type_str(f.type)}")
    return ", ".join(fields)


def get_arrow_from_batches(
    batches: Optional[List[ResultBatch]],
    schema: None = None,
    infer_nested_types: bool = False,
) -> pa.Table:
    if batches is None or len(batches) == 0:
        if schema is not None:
            return (
                schema if isinstance(schema, Schema) else Schema(schema)
            ).create_empty_arrow_table()
        raise ValueError("No result")
    nested_cols = _get_nested_columns(batches[0])
    adf = pa.concat_tables([x.to_arrow() for x in batches])
    if adf.num_rows == 0:
        return fix_snowflake_arrow_result(adf)
    if schema is None:
        adf = fix_snowflake_arrow_result(adf)
        if infer_nested_types and len(nested_cols) > 0:
            adf = parse_json_columns(adf, nested_cols)
        return adf
    _schema = schema if isinstance(schema, Schema) else Schema(schema)
    adf = parse_json_columns(adf, nested_cols)
    func = get_alter_func(adf.schema, _schema.pa_schema, safe=True)
    return func(adf)


def _get_nested_columns(batch: ResultBatch) -> List[str]:
    res: List[str] = []
    for meta in batch.schema:
        f = FIELD_TYPES[meta.type_code]
        if f.name in ["OBJECT", "ARRAY", "MAP", "VARIANT"]:
            res.append(meta.name)
    return res
