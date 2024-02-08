from typing import Any

import fugue.api as fa
from fugue import AnyDataFrame, AnyExecutionEngine, ArrowDataFrame
from triad import Schema

from ._utils import is_select_query
from .client import SnowflakeClient
from .dataframe import SnowflakeDataFrame
from .execution_engine import SnowflakeExecutionEngine


def get_schema(query_or_table: str) -> Schema:
    """Get the schema of certain query or table

    :param query_or_table: the table name or query string
    :return: the schema of the output
    """
    client = SnowflakeClient.get_or_create(fa.get_current_conf())
    tdf = SnowflakeDataFrame(client.query_or_table_to_ibis(query_or_table))
    return tdf.schema


def load(
    query_or_table: str,
    sample: float = 1.0,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> AnyDataFrame:
    """Load Snowflake table or SQL result as a dataframe

    :param query_or_table: the query string or table name
    :param sample: sample percentage, defaults to 1.0
    :param engine: execution engine, defaults to None (the current execution engine)
    :param engine_conf: engine config, defaults to None
    :param as_fugue: whether output a Fugue DataFrame, defaults to False

    :return: the output as a dataframe

    .. admonition:: Examples

        .. code-block:: python

            import fugue.api as fa
            import fugue_snowflake.api as fsa

            sql = '''
                SELECT
                    name, gender,
                    SUM(number) AS total
                FROM
                    DB.SCHEMA.TABLE
                GROUP BY name, gender
            '''

            # direct load
            t1 = fsa.load("DB.SCHEMA.TABLE")  # t1 is an ibis table
            t2 = fsa.load(sql, as_fugue=True)  # t2 is a SnowflakeDataFrame

            # load under an engine
            with fa.engine_context(spark_session):
                t3 = fsa.load(sql)  # t3 is a pyspark DataFrame
                # loading parallelism will be at most 4
                t4 = fsa.load(sql, parallelism=4)

            # load sql result as a pandas dataframe
            fa.as_pandas(fsa.load(sql))

            # if you have a distributed backend, for example Ray, this could be faster
            fa.as_pandas(fsa.load(sql, engine="ray"))
    """
    qot = query_or_table
    if sample < 1.0:
        qot = f"SELECT * FROM ({qot}) SAMPLE BERNOULLI({sample * 100})"
    query = qot
    if not is_select_query(qot):
        query = f"SELECT * FROM {qot}"
    with fa.engine_context(engine, engine_conf=engine_conf, infer_by=["force_sf"]) as e:
        print(e)
        client = SnowflakeClient.get_or_create(fa.get_current_conf())
        if isinstance(e, SnowflakeExecutionEngine):
            print(qot)
            res: Any = client.query_or_table_to_ibis(qot)
            return SnowflakeDataFrame(res) if as_fugue else res
        elif not e.is_distributed and e.get_current_parallelism() <= 1:
            adf = client.query_to_arrow(query)
            return ArrowDataFrame(adf) if as_fugue else adf
        else:
            tb = client.query_to_engine_df(query, engine=e)
            return tb if as_fugue else fa.get_native_as_df(tb)
