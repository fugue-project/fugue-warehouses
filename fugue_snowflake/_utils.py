from fugue_ibis._utils import to_schema as _to_schema
from fugue_ibis import IbisSchema
from triad import Schema


def to_schema(schema: IbisSchema) -> Schema:
    return _to_schema(schema)
