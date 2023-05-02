import sys
from datetime import date, datetime
from typing import Any

import fugue.api as fa
import pandas as pd
import pytest
from fugue import ArrowDataFrame
from fugue_test.dataframe_suite import DataFrameTests

from fugue_trino import TrinoDataFrame

from ._utils import MockTrinoExecutionEngine, get_testing_client


@pytest.mark.skipif(sys.version_info < (3, 8), reason="< 3.8")
class TrinoDataFrameTests(DataFrameTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._client = get_testing_client()
        cls._engine = MockTrinoExecutionEngine(cls._client)
        cls._cache = {}

    @classmethod
    def tearDownClass(cls):
        cls._client.stop()

    def df(self, data: Any = None, schema: Any = None) -> TrinoDataFrame:
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

    def test_binary_type(self):
        pass

    def test_struct_type(self):
        pass

    def test_as_arrow(self):
        # empty
        df = self.df([], "a:int,b:int")
        assert [] == list(ArrowDataFrame(fa.as_arrow(df)).as_dict_iterable())
        assert fa.is_local(fa.as_arrow(df))
        # pd.Nat
        df = self.df([[pd.NaT, 1]], "a:datetime,b:int")
        assert [dict(a=None, b=1)] == list(
            ArrowDataFrame(fa.as_arrow(df)).as_dict_iterable()
        )
        # pandas timestamps
        df = self.df([[pd.Timestamp("2020-01-01"), 1]], "a:datetime,b:int")
        assert [dict(a=datetime(2020, 1, 1), b=1)] == list(
            ArrowDataFrame(fa.as_arrow(df)).as_dict_iterable()
        )

    def test_alter_columns(self):
        # empty
        df = self.df([], "a:str,b:int")
        ndf = fa.alter_columns(df, "a:str,b:str")
        assert [] == fa.as_array(ndf, type_safe=True)
        assert fa.get_schema(ndf) == "a:str,b:str"

        # no change
        df = self.df([["a", 1], ["c", None]], "a:str,b:int")
        ndf = fa.alter_columns(df, "b:int,a:str", as_fugue=True)
        assert [["a", 1], ["c", None]] == fa.as_array(ndf, type_safe=True)
        assert fa.get_schema(ndf) == "a:str,b:int"

        # bool -> str
        df = self.df([["a", True], ["b", False], ["c", None]], "a:str,b:bool")
        ndf = fa.alter_columns(df, "b:str", as_fugue=True)
        actual = fa.as_array(ndf, type_safe=True)
        # Capitalization doesn't matter
        # and dataframes don't need to be consistent on capitalization
        expected1 = [["a", "True"], ["b", "False"], ["c", None]]
        expected2 = [["a", "true"], ["b", "false"], ["c", None]]
        assert expected1 == actual or expected2 == actual
        assert fa.get_schema(ndf) == "a:str,b:str"

        # int -> str
        df = self.df([["a", 1], ["c", None]], "a:str,b:int")
        ndf = fa.alter_columns(df, "b:str", as_fugue=True)
        arr = fa.as_array(ndf, type_safe=True)
        assert [["a", "1"], ["c", None]] == arr or [
            ["a", "1.0"],
            ["c", None],
        ] == arr  # in pandas case, it can't treat [1, None] as an int col
        assert fa.get_schema(ndf) == "a:str,b:str"

        # int -> double
        df = self.df([["a", 1], ["c", None]], "a:str,b:int")
        ndf = fa.alter_columns(df, "b:double", as_fugue=True)
        assert [["a", 1], ["c", None]] == fa.as_array(ndf, type_safe=True)
        assert fa.get_schema(ndf) == "a:str,b:double"

        # double -> str
        df = self.df([["a", 1.1], ["b", None]], "a:str,b:double")
        data = fa.as_array(fa.alter_columns(df, "b:str", as_fugue=True), type_safe=True)
        assert [["a", "1.1E0"], ["b", None]] == data

        # double -> int
        df = self.df([["a", 1.0], ["b", None]], "a:str,b:double")
        data = fa.as_array(fa.alter_columns(df, "b:int", as_fugue=True), type_safe=True)
        assert [["a", 1], ["b", None]] == data

        # date -> str
        df = self.df(
            [["a", date(2020, 1, 1)], ["b", date(2020, 1, 2)], ["c", None]],
            "a:str,b:date",
        )
        data = fa.as_array(fa.alter_columns(df, "b:str", as_fugue=True), type_safe=True)
        assert [["a", "2020-01-01"], ["b", "2020-01-02"], ["c", None]] == data

        # datetime -> str
        df = self.df(
            [
                ["a", datetime(2020, 1, 1, 3, 4, 5)],
                ["b", datetime(2020, 1, 2, 16, 7, 8)],
                ["c", None],
            ],
            "a:str,b:datetime",
        )
        data = fa.as_array(fa.alter_columns(df, "b:str", as_fugue=True), type_safe=True)
        assert [
            ["a", "2020-01-01 03:04:05.000"],
            ["b", "2020-01-02 16:07:08.000"],
            ["c", None],
        ] == data

        # str -> bool
        df = self.df([["a", "trUe"], ["b", "False"], ["c", None]], "a:str,b:str")
        ndf = fa.alter_columns(df, "b:bool,a:str", as_fugue=True)
        assert [["a", True], ["b", False], ["c", None]] == fa.as_array(
            ndf, type_safe=True
        )
        assert fa.get_schema(ndf) == "a:str,b:bool"

        # str -> int
        df = self.df([["a", "1"]], "a:str,b:str")
        ndf = fa.alter_columns(df, "b:int,a:str")
        assert [["a", 1]] == fa.as_array(ndf, type_safe=True)
        assert fa.get_schema(ndf) == "a:str,b:int"

        # str -> double
        df = self.df([["a", "1.1"], ["b", "2"], ["c", None]], "a:str,b:str")
        ndf = fa.alter_columns(df, "b:double", as_fugue=True)
        assert [["a", 1.1], ["b", 2.0], ["c", None]] == fa.as_array(ndf, type_safe=True)
        assert fa.get_schema(ndf) == "a:str,b:double"

        # str -> date  # doesn't work in trino
        # df = self.df(
        #     [["1", "2020-01-01"], ["2", "2020-01-02 01:02:03"], ["3", None]],
        #     "a:str,b:str",
        # )
        # ndf = fa.alter_columns(df, "b:date,a:int", as_fugue=True)
        # assert [
        #     [1, date(2020, 1, 1)],
        #     [2, date(2020, 1, 2)],
        #     [3, None],
        # ] == fa.as_array(ndf, type_safe=True)
        # assert fa.get_schema(ndf) == "a:int,b:date"

        # str -> datetime
        df = self.df(
            [["1", "2020-01-01"], ["2", "2020-01-02 01:02:03"], ["3", None]],
            "a:str,b:str",
        )
        ndf = fa.alter_columns(df, "b:datetime,a:int", as_fugue=True)
        assert [
            [1, datetime(2020, 1, 1)],
            [2, datetime(2020, 1, 2, 1, 2, 3)],
            [3, None],
        ] == fa.as_array(ndf, type_safe=True)
        assert fa.get_schema(ndf) == "a:int,b:datetime"
