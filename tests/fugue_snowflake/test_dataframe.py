import sys
from datetime import date, datetime
from typing import Any

import pandas as pd
import pytest
from fugue import ArrowDataFrame
from fugue_test.dataframe_suite import DataFrameTests

from fugue_snowflake import SnowflakeClient, SnowflakeDataFrame

from .mock_engine import MockSnowflakeExecutionEngine


@pytest.mark.skipif(sys.version_info < (3, 8), reason="< 3.8")
class SnowflakeDataFrameTests(DataFrameTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._client = SnowflakeClient.get_or_create(
            {"fugue.snowflake.credentials.env": "FUGUE_SNOWFLAKE_TEST_CRED"}
        )
        cls._engine = MockSnowflakeExecutionEngine(cls._client)
        cls._cache = {}

    @classmethod
    def tearDownClass(cls):
        cls._client.stop()

    def df(self, data: Any = None, schema: Any = None) -> SnowflakeDataFrame:
        return self._engine.sql_engine.to_df(data, schema)

    def test_is_not_local(self):
        df = self.df([["x", 1]], "a:str,b:int")
        assert not df.is_local
        assert df.is_bounded

    def test_map_type(self):
        pass

    def test_deep_nested_types(self):
        pass

    def test_list_type(self):
        # TODO: this doesn't work, is this a bug of bigquery upload?
        pass

    def test_as_arrow(self):
        # empty
        df = self.df([], "a:int,b:int")
        assert [] == list(ArrowDataFrame(df.as_arrow()).as_dict_iterable())
        # pd.Nat
        df = self.df([[pd.NaT, 1]], "a:datetime,b:int")
        assert [dict(a=None, b=1)] == list(
            ArrowDataFrame(df.as_arrow()).as_dict_iterable()
        )
        # pandas timestamps
        df = self.df([[pd.Timestamp("2020-01-01"), 1]], "a:datetime,b:int")
        assert [dict(a=datetime(2020, 1, 1), b=1)] == list(
            ArrowDataFrame(df.as_arrow()).as_dict_iterable()
        )

    def test_alter_columns(self):
        # empty
        df = self.df([], "a:str,b:int")
        ndf = df.alter_columns("a:str,b:str")
        assert [] == ndf.as_array(type_safe=True)
        assert ndf.schema == "a:str,b:str"

        # no change
        df = self.df([["a", 1], ["c", None]], "a:str,b:int")
        ndf = df.alter_columns("b:int,a:str")
        assert [["a", 1], ["c", None]] == ndf.as_array(type_safe=True)
        assert ndf.schema == df.schema

        # bool -> str
        df = self.df([["a", True], ["b", False], ["c", None]], "a:str,b:bool")
        ndf = df.alter_columns("b:str")
        actual = ndf.as_array(type_safe=True)
        # Capitalization doesn't matter
        # and dataframes don't need to be consistent on capitalization
        expected1 = [["a", "True"], ["b", "False"], ["c", None]]
        expected2 = [["a", "true"], ["b", "false"], ["c", None]]
        assert expected1 == actual or expected2 == actual
        assert ndf.schema == "a:str,b:str"

        # int -> str
        df = self.df([["a", 1], ["c", None]], "a:str,b:int")
        ndf = df.alter_columns("b:str")
        assert [["a", "1"], ["c", None]] == ndf.as_array(type_safe=True)
        assert ndf.schema == "a:str,b:str"

        # int -> double
        df = self.df([["a", 1], ["c", None]], "a:str,b:int")
        ndf = df.alter_columns("b:double")
        assert [["a", 1], ["c", None]] == ndf.as_array(type_safe=True)
        assert ndf.schema == "a:str,b:double"

        # double -> str
        df = self.df([["a", 1.1], ["b", None]], "a:str,b:double")
        data = df.alter_columns("b:str").as_array(type_safe=True)
        assert [["a", "1.1"], ["b", None]] == data

        # double -> int
        df = self.df([["a", 1.0], ["b", None]], "a:str,b:double")
        data = df.alter_columns("b:int").as_array(type_safe=True)
        assert [["a", 1], ["b", None]] == data

        # date -> str
        df = self.df(
            [["a", date(2020, 1, 1)], ["b", date(2020, 1, 2)], ["c", None]],
            "a:str,b:date",
        )
        data = df.alter_columns("b:str").as_array(type_safe=True)
        assert [["a", "2020-01-01"], ["b", "2020-01-02"], ["c", None]] == data

        # str -> bool
        df = self.df([["a", "trUe"], ["b", "False"], ["c", None]], "a:str,b:str")
        ndf = df.alter_columns("b:bool,a:str")
        assert [["a", True], ["b", False], ["c", None]] == ndf.as_array(type_safe=True)
        assert ndf.schema == "a:str,b:bool"

        # str -> int
        df = self.df([["a", "1"]], "a:str,b:str")
        ndf = df.alter_columns("b:int,a:str")
        assert [["a", 1]] == ndf.as_array(type_safe=True)
        assert ndf.schema == "a:str,b:int"

        # str -> double
        df = self.df([["a", "1.1"], ["b", "2"], ["c", None]], "a:str,b:str")
        ndf = df.alter_columns("b:double")
        assert [["a", 1.1], ["b", 2.0], ["c", None]] == ndf.as_array(type_safe=True)
        assert ndf.schema == "a:str,b:double"

    def test_alter_columns_datetime(self):
        # datetime -> str
        df = self.df(
            [
                ["a", datetime(2020, 1, 1, 3, 4, 5)],
                ["b", datetime(2020, 1, 2, 16, 7, 8)],
                ["c", None],
            ],
            "a:str,b:datetime",
        )
        data = df.alter_columns("b:str").as_array(type_safe=True)
        assert [
            ["a", "2020-01-01 03:04:05+00"],
            ["b", "2020-01-02 16:07:08+00"],
            ["c", None],
        ] == data

        # str -> date
        df = self.df(
            [["1", "2020-01-01"], ["2", "2020-01-02"], ["3", None]],
            "a:str,b:str",
        )
        ndf = df.alter_columns("b:date,a:int")
        assert [
            [1, date(2020, 1, 1)],
            [2, date(2020, 1, 2)],
            [3, None],
        ] == ndf.as_array(type_safe=True)
        assert ndf.schema == "a:int,b:date"

        # str -> datetime
        df = self.df(
            [["1", "2020-01-01"], ["2", "2020-01-02 01:02:03"], ["3", None]],
            "a:str,b:str",
        )
        ndf = df.alter_columns("b:datetime,a:int")
        assert [
            [1, datetime(2020, 1, 1)],
            [2, datetime(2020, 1, 2, 1, 2, 3)],
            [3, None],
        ] == ndf.as_array(type_safe=True)
        assert ndf.schema == "a:int,b:datetime"
