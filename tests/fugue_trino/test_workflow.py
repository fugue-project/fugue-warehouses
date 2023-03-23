import fugue.api as fa
import pandas as pd
from fugue import FugueWorkflow
from fugue_test.builtin_suite import BuiltInTests

from ._utils import MockTrinoExecutionEngine, get_testing_client


class TrinoWorkflowTests(BuiltInTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._client = get_testing_client()
        cls._engine = cls.make_engine(cls)
        fa.set_global_engine(cls._engine)

    @classmethod
    def tearDownClass(cls):
        fa.clear_global_engine()
        cls._client.stop()

    def make_engine(self):
        return MockTrinoExecutionEngine(self._client, conf={"test": True})

    def test_transform_binary(self):
        pass

    def test_select(self):
        with FugueWorkflow() as dag:
            a = dag.df([[1, 10], [2, 20], [3, 30]], "x:long,y:long")
            b = dag.df([[2, 20, 40], [3, 30, 90]], "x:long,y:long,z:long")
            dag.select("* FROM", a).assert_eq(a)
            dag.select(a, ".* FROM", a).assert_eq(a)
            dag.select("SELECT *,x*y AS z FROM", a, "WHERE x>=2").assert_eq(b)

            c = dag.df([[2, 20, 40], [3, 30, 90]], "x:long,y:long,zb:long")
            dag.select(
                "  SELECT t1.*,z AS zb FROM ",
                a,
                "AS t1 INNER JOIN",
                b,
                "AS t2 ON t1.x=t2.x  ",
            ).assert_eq(c)

            # no select
            dag.select(
                "t1.*,z AS zb FROM ", a, "AS t1 INNER JOIN", b, "AS t2 ON t1.x=t2.x"
            ).assert_eq(c)

            # somehow qpd doesn't work here, duckdb is fine
            # specify sql engine
            dag.select(
                "SELECT t1.*,z AS zb FROM ",
                a,
                "AS t1 INNER JOIN",
                b,
                "AS t2 ON t1.x=t2.x",
                sql_engine="duckdb",
            ).assert_eq(c)

            # no input
            dag.select("'test' AS a").assert_eq(dag.df([["test"]], "a:str"))

            # make sure transform -> select works
            b = a.transform(mock_tf1)
            a = a.transform(mock_tf1)
            aa = dag.select("* FROM", a)
            dag.select("* FROM", b).assert_eq(aa)
        dag.run(self.engine)


# schema: *,ct:int,p:int
def mock_tf1(df: pd.DataFrame, p=1) -> pd.DataFrame:
    df["ct"] = df.shape[0]
    df["p"] = p
    return df
