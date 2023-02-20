from unittest import TestCase

import fugue.api as fa
import pandas as pd
import pyarrow as pa
from fugue import ExecutionEngine, LocalDataFrame
from fugue_dask import DaskDataFrame

import fugue_bigquery.api as fbqa
from fugue_bigquery import BigQueryClient, BigQueryDataFrame, BigQueryExecutionEngine

from .mock_engine import MockBigQueryExecutionEngine


class APITests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls._client = BigQueryClient.get_or_create(
            {"fugue.bq.credentials.env": "FUGUE_GOOGLE_TEST_CRED"}
        )
        df = pa.Table.from_pandas(pd.DataFrame(dict(a=["a", "b", "c"], b=range(3))))
        cls._tb = cls._client.arrow_to_table(df, expiration="20min")
        cls._engine = BigQueryExecutionEngine()
        fa.set_global_engine(cls._engine)

    @property
    def engine(self) -> ExecutionEngine:
        return self._engine  # type: ignore

    @property
    def tb1(self) -> str:
        return self._tb

    @classmethod
    def tearDownClass(cls):
        fa.clear_global_engine()
        cls._client.stop()

    def make_engine(self):
        return MockBigQueryExecutionEngine(self._client, conf={"test": True})

    def test_load_table(self):
        tb = self.tb1

        assert fbqa.get_schema(tb) == "a:str,b:long"
        df = fbqa.load_table(tb, as_fugue=True)
        assert isinstance(df, BigQueryDataFrame)
        assert df.schema == "a:str,b:long"
        assert df.count() == 3
        df.show()

        df = fbqa.load_table(tb, columns=["b"], as_fugue=True)
        assert isinstance(df, BigQueryDataFrame)
        assert df.schema == "b:long"
        assert df.count() == 3

        df = fbqa.load_table(tb, columns=["b"], sample=0.99, as_fugue=True)
        assert isinstance(df, BigQueryDataFrame)
        assert df.schema == "b:long"

        df = fbqa.load_sql(f"SELECT * FROM `{tb}` WHERE b>=2", as_fugue=True)
        assert isinstance(df, BigQueryDataFrame)
        assert df.count() == 1
        assert df.as_array() == [["c", 2]]

        df = fbqa.load_sql(f"SELECT * FROM `{tb}` WHERE b>=3", as_fugue=True)
        assert isinstance(df, BigQueryDataFrame)
        assert df.count() == 0

    def test_load_in_native_engine(self):
        tb = self.tb1

        with fa.engine_context("pandas"):
            df = fa.as_fugue_df(("bq", tb))
            assert isinstance(df, LocalDataFrame)
            assert df.count() == 3

            df = fa.as_fugue_df(("bq", f"SELECT b FROM `{tb}` WHERE b>=2"))
            assert isinstance(df, LocalDataFrame)
            assert df.count() == 1
            assert df.as_array() == [[2]]

            def func(df: pd.DataFrame) -> pd.DataFrame:
                return df[df.b >= 2]

            res = fa.as_array(fa.transform(("bq", tb), func, schema="*"))
            assert res == [["c", 2]]

    def test_load_in_distributed_engine(self):
        tb = self.tb1

        with fa.engine_context("dask"):
            df = fbqa.load_table(tb, as_fugue=True)
            assert isinstance(df, DaskDataFrame)
            assert df.count() == 3

            df = fbqa.load_sql(f"SELECT b FROM `{tb}` WHERE b>=2", as_fugue=True)
            assert isinstance(df, DaskDataFrame)
            assert df.count() == 1
            assert df.as_array() == [[2]]
