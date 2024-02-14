from typing import Any

import ibis.expr.datatypes as dt
import sqlalchemy.types as sat
from ibis.backends.snowflake import Backend as BaseBackend
from ibis.backends.snowflake import (
    SnowflakeCompiler,
    SnowflakeExprTranslator,
    SnowflakeType,
)


class FugueSnowflakeType(SnowflakeType):
    @classmethod
    def to_ibis(cls, typ: Any, nullable: bool = True):
        if isinstance(typ, (sat.BINARY, sat.VARBINARY)):
            return dt.Binary(nullable=nullable)
        return super().to_ibis(typ, nullable=nullable)


class FugueSnowflakeExprTranslator(SnowflakeExprTranslator):
    type_mapper = FugueSnowflakeType


class FugueSnowflakeCompiler(SnowflakeCompiler):
    translator_class = FugueSnowflakeExprTranslator


class Backend(BaseBackend):
    compiler = FugueSnowflakeCompiler
