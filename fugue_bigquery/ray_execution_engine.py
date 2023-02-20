from typing import Any

from fugue import DataFrame, ExecutionEngine
from fugue.plugins import parse_execution_engine
from fugue_ibis import IbisDataFrame, IbisTable
from fugue_ray import RayExecutionEngine
from triad import ParamDict

from .client import BigQueryClient
from .execution_engine import BigQueryExecutionEngine


class BigQueryRayExecutionEngine(BigQueryExecutionEngine):
    def create_non_ibis_execution_engine(self) -> ExecutionEngine:
        return RayExecutionEngine(self.conf)

    def _to_non_ibis_dataframe(self, df: Any, schema: Any = None) -> DataFrame:
        if isinstance(df, (IbisTable, IbisDataFrame)):
            sql = df.compile() if isinstance(df, IbisTable) else df.native.compile()
            # print(sql)
            tbn = self.sql_engine.client.query_to_table(sql)
            return self.sql_engine.client.read_table(
                self.non_ibis_engine,
                tbn,
                max_stream_count=self.non_ibis_engine.get_current_parallelism() * 2,
            )
        return super()._to_non_ibis_dataframe(df, schema)

    def __repr__(self) -> str:
        return "BigQueryRayExecutionEngine"


@parse_execution_engine.candidate(
    matcher=lambda engine, conf, **kwargs: isinstance(engine, str)
    and (engine == "bq_ray" or engine == "bigquery_ray"),
    priority=2.5,
)
def _parse_bq_ray(engine: str, conf: Any, **kwargs) -> ExecutionEngine:
    _conf = ParamDict(conf)
    client = BigQueryClient.get_or_create(_conf)
    return BigQueryRayExecutionEngine(client, _conf)
