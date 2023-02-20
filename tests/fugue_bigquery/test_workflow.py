import fugue.api as fa
from fugue import FugueWorkflow
from fugue.exceptions import FugueWorkflowCompileError
from fugue_test.builtin_suite import BuiltInTests
from pytest import raises

from fugue_bigquery import BigQueryClient

from .mock_engine import MockBigQueryExecutionEngine


class BigQueryWorkflowTests(BuiltInTests.Tests):
    @classmethod
    def setUpClass(cls):
        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/usr/fugue.json"
        cls._client = BigQueryClient.get_or_create(
            {"fugue.bq.credentials.env": "FUGUE_GOOGLE_TEST_CRED"}
        )
        cls._engine = cls.make_engine(cls)
        fa.set_global_engine(cls._engine)

    @classmethod
    def tearDownClass(cls):
        fa.clear_global_engine()
        cls._client.stop()

    def make_engine(self):
        return MockBigQueryExecutionEngine(self._client, conf={"test": True})

    def test_any_column_name(self):
        # https://cloud.google.com/bigquery/docs/schemas#column_names
        # bigquery does not support arbitrary column names
        pass

    def test_yield_table(self):
        with raises(FugueWorkflowCompileError):
            FugueWorkflow().df([[0]], "a:long").checkpoint().yield_table_as("x")

        with raises(ValueError):
            FugueWorkflow().df([[0]], "a:long").persist().yield_table_as("x")

        def run_test(deterministic):
            dag1 = FugueWorkflow()
            df = dag1.df([[0]], "a:long")
            if deterministic:
                df = df.deterministic_checkpoint(storage_type="table")
            df.yield_table_as("x")
            id1 = dag1.spec_uuid()
            dag2 = FugueWorkflow()
            dag2.df([[0]], "a:long").assert_eq(dag2.df(dag1.yields["x"]))
            id2 = dag2.spec_uuid()
            dag1.run(self.engine)
            dag2.run(self.engine)
            return id1, id2

        id1, id2 = run_test(False)
        id3, id4 = run_test(False)
        assert id1 == id3
        assert id2 != id4  # non deterministic yield (direct yield)

        id1, id2 = run_test(True)
        id3, id4 = run_test(True)
        assert id1 == id3
        assert id2 == id4  # deterministic yield (yield deterministic checkpoint)
