import fugue.api as fa
from fugue_snowflake import SnowflakeClient
import datetime


def test_upload_download(snowflake_session: SnowflakeClient):
    def _assert(data, schema, schema2=None):
        df = fa.as_fugue_df(data, schema=schema)
        with fa.engine_context("pandas") as e:
            tb = snowflake_session.df_to_temp_table(df, engine=e)
        adf = snowflake_session.query_to_arrow(
            "SELECT * FROM " + tb, infer_nested_types=True
        )
        assert data == fa.as_array(adf)
        if schema2 is not None:
            adf = snowflake_session.query_to_arrow(
                "SELECT * FROM " + tb, schema=schema2
            )
            assert data == fa.as_array(adf)

    # simple types
    _assert([[1, "a", True, 1.1], [4, "b", False, 2.2]], "a:long,b:str,c:bool,d:double")
    # binary
    _assert([[1, b"abc"]], "a:long,b:bytes")
    # date
    _assert([[1, datetime.date(2020, 1, 1)]], "a:long,b:date")
    # datetime
    _assert([[1, datetime.datetime.now()]], "a:long,b:datetime")
    # datetime + tz
    _assert(
        [[1, datetime.datetime.now(tz=datetime.timezone.utc)]],
        "a:long,b:timestamp(us,UTC)",
    )
    # array
    _assert([[1, [2, 3]], [4, [5, 6]]], "a:long,b:[long]", "a:int,b:[int]")
    # dict
    _assert([[1, {"a": 1}], [4, {"a": 2}]], "a:long,b:{a:long}", "a:int,b:{a:int}")
