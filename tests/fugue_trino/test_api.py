import fugue.api as fa
from ._utils import get_testing_client
import pandas as pd


def test_api():
    def tr(df: pd.DataFrame) -> pd.DataFrame:
        return df

    with get_testing_client() as client:
        df1 = fa.as_fugue_df([["a", 1], ["b", 2]], schema="x:str,b:long")
        tb1 = str(client.df_to_table(df1))
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
