import fugue.api as fa
import pandas as pd
from fugue_ibis import IbisTable

import fugue_trino.api as fta
from fugue_trino import TrinoDataFrame

from ._utils import get_testing_client
import ray
from fugue_ray import RayDataFrame
import ray.data as rd


def test_fugue_api():
    def tr(df: pd.DataFrame) -> pd.DataFrame:
        return df

    with get_testing_client() as client:
        df1 = fa.as_fugue_df([["a", 1], ["b", 2]], schema="x:str,b:long")
        tb1 = str(client.df_to_table(df1))
        assert fa.get_schema(("trino", tb1)) == "x:str,b:long"
        assert fa.get_schema(client.query_to_ibis(tb1)) == "x:str,b:long"
        df2 = fa.as_fugue_df([["a", True], ["c", False]], schema="x:str,c:bool")
        tb2 = str(client.df_to_table(df2))
        fa.show(("trino", tb1))
        res = fa.inner_join(("trino", tb1), ("trino", tb2), as_fugue=True)
        assert [["a", 1, True]] == res.as_array()
        assert [["b", 2]] == fa.as_array(("trino", f"SELECT * FROM {tb1} WHERE b=2"))
        assert 1 == fa.count(("trino", f"SELECT * FROM {tb1} WHERE b=2"))
        res = fa.transform(res, tr, schema="*")
        assert [["a", 1, True]] == res.as_array()
        assert 2 == fa.count(fa.transform(("trino", tb1), tr, schema="*"))


def test_load():
    with get_testing_client() as client:
        df1 = fa.as_fugue_df([["a", 1], ["b", 2]], schema="x:str,b:long")
        tb1 = str(client.df_to_table(df1))
        with fa.engine_context(client):
            df = fta.load(tb1)
            assert isinstance(df, IbisTable)
            df = fta.load(tb1, as_fugue=True)
            assert isinstance(df, TrinoDataFrame)

        with ray.init():
            with fa.engine_context("ray"):
                df = fta.load(tb1, parallelism=5)
                assert isinstance(df, rd.Dataset)
                df = fta.load(tb1, parallelism=5, as_fugue=True)
                assert isinstance(df, RayDataFrame)
                assert df.num_partitions == 5
