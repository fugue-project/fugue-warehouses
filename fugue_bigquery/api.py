from typing import Any, List, Optional

import fugue.api as fa
from fugue import AnyDataFrame, AnyExecutionEngine
from triad import Schema
from triad.utils.schema import quote_name

from ._utils import table_to_query, is_select_query
from .client import BigQueryClient
from .dataframe import BigQueryDataFrame
from .execution_engine import BigQueryExecutionEngine


def get_schema(query_or_table: str) -> Schema:
    """Get the schema of certain query or table

    :param query_or_table: the table name or query string
    :return: the schema of the output
    """
    query = (
        query_or_table  # query
        if is_select_query(query_or_table)
        else f"SELECT * FROM {quote_name(query_or_table)}"
    )
    client = BigQueryClient.get_or_create(fa.get_current_conf())
    tdf = BigQueryDataFrame(client.ibis.sql(query))
    return tdf.schema


def load_table(
    table: str,
    columns: Optional[List[str]] = None,
    row_filter: Optional[str] = None,
    sample: float = 1.0,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    parallelism: Optional[int] = None,
    **read_kwargs: Any,
) -> AnyDataFrame:
    """Load BigQuery table using the current execution engine.

    :param table: the BigQuery table name
    :param columns: columns to load, defaults to None
    :param row_filter: row filter, defaults to None
    :param sample: sample percentage, defaults to 1.0
    :param engine: execution engine, defaults to None (the current execution engine)
    :param engine_conf: engine config, defaults to None
    :param as_fugue: whether output a Fugue DataFrame, defaults to False
    :param parallelism: the parallelism to load the BigQuery output,
        defaults to None (determined by the current engine's parallelism)
    :return: the output as a dataframe

    .. note::

        ``sample`` can significantly reduce the loading data size, and it
        reduces cost.

    .. admonition:: Examples

        .. code-block:: python

            import fugue.api as fa
            import fugue_bigquery.api as fbqa

            table = "bigquery-public-data.usa_names.usa_1910_2013"

            # direct load
            t1 = fbqa.load_table(table)  # t1 is an ibis table
            t2 = fbqa.load_table(table, as_fugue=True)  # t2 is a BigQueryDataFrame

            # load under an engine
            with fa.engine_context(spark_session):
                t3 = fbqa.load_table(table)  # t3 is a pyspark DataFrame
                # loading parallelism will be at most 4
                t4 = fbqa.load_table(table, parallelism=4)

            # a good way to get sample table
            fa.as_pandas(fbqa.load_table(table, sample=0.001))

            # if you have a distributed backend, for example Ray, this could be faster
            fa.as_pandas(fbqa.load_table(table, sample=0.001, engine="ray"))
    """
    if sample < 1.0:
        return load_sql(
            table_to_query(
                table, columns=columns, row_filter=row_filter, sample=sample
            ),
            engine=engine,
            engine_conf=engine_conf,
            as_fugue=as_fugue,
            parallelism=parallelism,
            **read_kwargs,
        )
    with fa.engine_context(engine, engine_conf=engine_conf, infer_by=["force_bq"]) as e:
        if isinstance(e, BigQueryExecutionEngine) and row_filter is None:
            tb = e.sql_engine.client.table_to_ibis(table, columns)
            return e.to_df(tb) if as_fugue else tb
        else:
            client = BigQueryClient.get_or_create(fa.get_current_conf())
            if parallelism is None:
                parallelism = e.get_current_parallelism() * 2
            res = client.read_table(
                e,
                table,
                columns=columns,
                row_filter=row_filter,
                max_stream_count=parallelism,
                **read_kwargs,
            )
            return res if as_fugue else fa.get_native_as_df(res)


def load_sql(
    query: str,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    parallelism: Optional[int] = None,
    **read_kwargs: Any,
) -> AnyDataFrame:
    """Load BigQuery SQL result as a dataframe

    :param query: the BigQuery SQL query
    :param engine: execution engine, defaults to None (the current execution engine)
    :param engine_conf: engine config, defaults to None
    :param as_fugue: whether output a Fugue DataFrame, defaults to False
    :param parallelism: the parallelism to load the BigQuery output,
        defaults to None (determined by the current engine's parallelism)

    :return: the output as a dataframe

    .. admonition:: Examples

        .. code-block:: python

            import fugue.api as fa
            import fugue_bigquery.api as fbqa

            sql = '''
                SELECT
                    name, gender,
                    SUM(number) AS total
                FROM
                    `bigquery-public-data.usa_names.usa_1910_2013`
                GROUP BY name, gender
            '''

            # direct load
            t1 = fbqa.load_sql(sql)  # t1 is an ibis table
            t2 = fbqa.load_sql(sql, as_fugue=True)  # t2 is a BigQueryDataFrame

            # load under an engine
            with fa.engine_context(spark_session):
                t3 = fbqa.load_sql(sql)  # t3 is a pyspark DataFrame
                # loading parallelism will be at most 4
                t4 = fbqa.load_sql(sql, parallelism=4)

            # load sql result as a pandas dataframe
            fa.as_pandas(fbqa.load_sql(sql))

            # if you have a distributed backend, for example Ray, this could be faster
            fa.as_pandas(fbqa.load_sql(sql, engine="ray"))
    """
    with fa.engine_context(engine, engine_conf=engine_conf, infer_by=["force_bq"]) as e:
        if isinstance(e, BigQueryExecutionEngine):
            res: Any = e.sql_engine.query_to_table(query, {})
            return BigQueryDataFrame(res) if as_fugue else res
        elif not e.is_distributed and e.get_current_parallelism() <= 1:
            client = BigQueryClient.get_or_create(fa.get_current_conf())
            tdf = BigQueryDataFrame(client.ibis.sql(query)).as_local()
            return tdf if as_fugue else tdf.as_pandas()
        else:
            client = BigQueryClient.get_or_create(fa.get_current_conf())
            tb = client.query_to_table(query, is_view=False)
            if parallelism is None:
                parallelism = e.get_current_parallelism() * 2
            res = client.read_table(
                e,
                tb,
                max_stream_count=parallelism,
                **read_kwargs,
            )
            return res if as_fugue else fa.get_native_as_df(res)
