from fugue_snowflake._utils import (
    to_snowflake_schema,
    fix_snowflake_arrow_result,
    parse_table_name,
    quote_name,
    unquote_name,
    build_package_list,
)
from importlib.metadata import version as get_version
from triad import Schema
from fugue_snowflake.client import SnowflakeClient
from typing import Any
from pytest import raises


def test_quote_unquote_name():
    assert quote_name("a") == '"a"'
    assert quote_name('"a"') == '"""a"""'
    assert unquote_name('"""a"""') == '"a"'
    assert unquote_name('"a"') == "a"
    assert unquote_name("a") == "a"


def test_parse_table_name():
    assert parse_table_name("") == []
    assert parse_table_name("a") == ["a"]
    assert parse_table_name('"a"') == ['"a"']
    assert parse_table_name("a.b.c") == ["a", "b", "c"]
    assert parse_table_name('a."b".c') == ["a", '"b"', "c"]
    assert parse_table_name('"a"."b"."c"') == ['"a"', '"b"', '"c"']
    assert parse_table_name('a."b"".x".c', normalize=True) == ["A", '"b"".x"', "C"]


def test_to_snowflake_schema():
    def _assert(s: Any, t: str):
        assert to_snowflake_schema(s) == t

    _assert(
        "a:int8,b:int16,c:int32,d:int64",
        '"a" BYTEINT, "b" TINYINT, "c" SMALLINT, "d" BIGINT',
    )
    _assert("a:uint8,b:uint16,c:uint32,d:uint64", '"a" INT, "b" INT, "c" INT, "d" INT')
    _assert(Schema("a:decimal(10,2)"), '"a" DECIMAL(10,2)')
    _assert("a:float16,b:float32,c:float64", '"a" FLOAT, "b" FLOAT, "c" FLOAT')

    _assert("a:bool,b:str,d:bytes", '"a" BOOLEAN, "b" STRING, "d" BINARY')

    _assert("a:date,b:datetime", '"a" DATE, "b" TIMESTAMP_NTZ')
    _assert("a:timestamp(ns,UTC)", '"a" TIMESTAMP_TZ')

    _assert("a:[int]", '"a" ARRAY')
    _assert("a:<int,str>", '"a" MAP')
    _assert("a:{a:int,b:str}", '"a" OBJECT')

    _assert("大:{`a b`:int,b:str}", '"大" OBJECT')

    with raises(NotImplementedError):
        to_snowflake_schema("a:null")


def test_to_snowflake_schema_with_temp_table():
    client = SnowflakeClient.get_or_create()

    def _assert(s: Any, t: str):
        ss = to_snowflake_schema(s)
        temp_table = "test1.s1.ttt"
        with client.cursor() as cur:
            cur.execute(f"CREATE OR REPLACE TEMP TABLE {temp_table} ({ss})")
            adf = client.query_to_arrow(
                f"SELECT * FROM {temp_table}", cursor=cur, infer_nested_types=False
            )
            cur.execute(f"DROP TABLE {temp_table}")
        assert Schema(t) == Schema(adf.schema)

    _assert("a:int8,b:int16,大:int32,D:int64", "a:long,b:long,大:long,D:long")
    _assert("a:uint8,b:uint16,c:uint32,D:uint64", "a:long,b:long,c:long,D:long")
    _assert("a:float16,b:float32,c:float64", "a:double,b:double,c:double")
    _assert("a:bool,b:str,d:bytes", "a:bool,b:str,d:bytes")
    _assert("a:date,b:datetime", "a:date,b:datetime")
    _assert("a:[long]", "a:[long]")

    # not supported by snowflake
    # _assert("a:decimal(10,2)", "a:decimal(10,2)")
    # _assert("a:timestamp(ns,UTC)", 'a:timestamp(ns,UTC)')


def test_build_package_list():
    fv = get_version("fugue")
    assert ["fugue==" + fv] == build_package_list(["fugue"])
    assert ["fugue>=0.9"] == build_package_list(["fugue>=0.9"])
    assert ["fugue>=0.9", "triad==0.5"] == build_package_list(
        ["fugue>=0.9", "triad==0.5", "fugue>=0.9"]
    )
