from typing import Any, Optional

import fugue.api as fa
from fugue import AnyDataFrame, AnyExecutionEngine

from .client import TrinoClient
from .dataframe import TrinoDataFrame
from .execution_engine import TrinoExecutionEngine


def load(
    query_or_table: str,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    parallelism: Optional[int] = None,
) -> AnyDataFrame:
    """Load Trino table using the current execution engine.

    :param query_or_table: the Trino query or table name
    :param engine: execution engine, defaults to None (the current execution engine)
    :param engine_conf: engine config, defaults to None
    :param as_fugue: whether output a Fugue DataFrame, defaults to False
    :param parallelism: the parallelism to load the BigQuery output,
        defaults to None (determined by the current engine's parallelism)
    :return: the output as a dataframe

    .. admonition:: Examples

        .. code-block:: python

            import fugue.api as fa
            import fugue_trino.api as fta

            table = "some.trino.table"

            # direct load
            t1 = fta.load(table)  # t1 is an ibis table
            t2 = fta.load(table, as_fugue=True)  # t2 is a TrinoDataFrame

            # load under an engine
            with fa.engine_context(spark_session):
                t3 = fta.load(table)  # t3 is a pyspark DataFrame
                # loading parallelism will be at most 4
                t4 = fta.load(table, parallelism=4)
    """
    with fa.engine_context(
        engine, engine_conf=engine_conf, infer_by=["force_trino"]
    ) as e:
        if isinstance(e, TrinoExecutionEngine):
            tb = e._client.query_to_ibis(query_or_table)
            return e.to_df(tb) if as_fugue else tb
        else:
            client = TrinoClient.get_or_create(fa.get_current_conf())
            df = TrinoDataFrame(client.query_to_ibis(query_or_table))
            res = e.to_df(df)
            if parallelism is not None and parallelism > 1:
                res = fa.repartition(res, partition=parallelism)
            return res if as_fugue else fa.get_native_as_df(res)
