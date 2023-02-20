import fugue.api as fa
from fugue_test.execution_suite import ExecutionEngineTests
from pytest import raises

from fugue_bigquery import BigQueryClient

from .mock_engine import MockBigQueryExecutionEngine


class BigQueryExecutionEngineTests(ExecutionEngineTests.Tests):
    @classmethod
    def setUpClass(cls):
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

    def _test_comap(self):
        # too long
        pass

    def _test_comap_with_key(self):
        # too long
        pass

    def _test_zip(self):
        # too long
        pass

    def test_sample(self):
        engine = self.engine
        a = engine.to_df([[x] for x in range(100)], "a:int")

        with raises(ValueError):
            engine.sample(a)  # must set one
        with raises(ValueError):
            engine.sample(a, n=90, frac=0.9)  # can't set both

        f = engine.sample(a, frac=0.8)
        assert f.schema == "a:int"
        assert f.count() > 50 and f.count() < 100

    def test_sample_n(self):
        engine = self.engine
        a = engine.to_df([[x] for x in range(100)], "a:int")

        b = engine.sample(a, n=90)
        assert b.schema == "a:int"
        assert b.count() == 90
