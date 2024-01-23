import json
import os
import tempfile
from contextvars import ContextVar
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

import ibis
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector
from snowflake.connector.constants import FIELD_TYPES
from fugue import (
    ArrayDataFrame,
    ArrowDataFrame,
    DataFrame,
    LocalDataFrame,
    AnyDataFrame,
)

# from ibis.formats.pyarrow import PyArrowSchema
from fugue_ibis import IbisTable
from triad import ParamDict, Schema, SerializableRLock, assert_or_throw

from ._constants import (
    FUGUE_SNOWFLAKE_CONF_CREDENTIALS_ENV,
)

_FUGUE_SNOWFLAKE_CLIENT_CONTEXT = ContextVar(
    "_FUGUE_SNOWFLAKE_CLIENT_CONTEXT", default=None
)

_CONTEXT_LOCK = SerializableRLock()


class SnowflakeClient:
    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        credentials_func: Optional[Callable[[], Dict[str, Any]]] = None,
    ):
        """Create a new SnowflakeClient.

        SnowflakeClient wraps a Snowflake connection and an Ibis connection.
        It also provides methods to convert between DataFrames and Snowflake tables.

        You can either provide the credentials directly with args or provide
        a dict with the credentials.

        :param account: Snowflake account name, optional.
        :param user: Snowflake user name, optional.
        :param password: Snowflake password, optional.
        :param database: Snowflake database name, optional.
        :param schema: Snowflake schema name, optional.
        :param role: Snowflake user role, optional.
        :param credentials_func: Callable returning a dictionary of credentials, optional.
        :raises AttributeError: If the required credentials are missing.
        """
        self._temp_tables: List[str] = []
        self._credentials_func = credentials_func

        if account and user and password and database and schema and role:
            self._account = account
            self._user = user
            self._password = password
            self._database = database
            self._schema = schema
            self._role = role
        elif credentials_func is not None:
            credentials = self._credentials_func()
            self._account = credentials["account"]
            self._user = credentials["user"]
            self._password = credentials["password"]
            self._database = credentials["database"]
            self._schema = credentials["schema"]
            self._role = credentials["role"]
        else:
            raise AttributeError(
                "Missing required credentials to connect to Snowflake."
            )

        self._sf = snowflake.connector.connect(
            account=self._account,
            user=self._user,
            password=self._password,
            database=self._database,
            schema=self._schema,
            role=self._role,
        )

        self._ibis = ibis.snowflake.connect(
            account=self._account,
            user=self._user,
            password=self._password,
            database=f"{self._database}/{self._schema}",
            role=self._role,
        )

    @staticmethod
    def get_or_create(conf: Any = None) -> "SnowflakeClient":
        """Get or create a SnowflakeClient instance.

        This static method returns an existing SnowflakeClient instance or creates a new one
        if none exists in the current context. The configuration can be provided to set up
        the client.

        :param conf: Configuration dictionary or None, defaults to None.
        :return: An instance of SnowflakeClient.
        """
        with _CONTEXT_LOCK:
            res = _FUGUE_SNOWFLAKE_CLIENT_CONTEXT.get()
            if res is None:
                _conf = ParamDict(conf)
                ce = _conf.get_or_none(FUGUE_SNOWFLAKE_CONF_CREDENTIALS_ENV, str)
                if ce is not None:
                    info = json.loads(open(os.environ[ce], "r").read())
                    credentials_func: Any = lambda: info
                else:
                    credentials_func = None
                res = SnowflakeClient(credentials_func=credentials_func)
                _FUGUE_SNOWFLAKE_CLIENT_CONTEXT.set(res)  # type: ignore
            return res

    @staticmethod
    def get_current() -> "SnowflakeClient":
        """Get the current SnowflakeClient instance.

        This static method returns the current SnowflakeClient instance if initialized,
        otherwise, it throws a ValueError.

        :return: An instance of SnowflakeClient.
        :raises ValueError: If no SnowflakeClient was initialized.
        """
        with _CONTEXT_LOCK:
            res = _FUGUE_SNOWFLAKE_CLIENT_CONTEXT.get()
            assert_or_throw(
                res is not None, ValueError("no SnowflakeClient was initialized")
            )
            return res  # type: ignore

    @property
    def sf(self) -> snowflake.connector.SnowflakeConnection:
        """Get the Snowflake connection.

        This property provides access to the underlying Snowflake connection.
        It can be used for operations that require direct interaction with the
        Snowflake database.

        :return: The Snowflake connection associated with this SnowflakeClient.
        """
        return self._sf

    def stop(self):
        """Stop the SnowflakeClient instance.

        This method drops any temporary tables created during the session and closes
        the Snowflake connection.
        """
        for tt in self._temp_tables:
            self._sf.cursor().execute(f"DROP TABLE IF EXISTS {tt}")
        self._sf.close()

    def __enter__(self) -> "SnowflakeClient":
        """Enter the runtime context for the SnowflakeClient object.

        This method returns the SnowflakeClient instance itself, allowing it to be used
        with the 'with' statement.

        :return: Self, the SnowflakeClient instance.
        """
        return self

    def __exit__(
        self, exception_type: Any, exception_value: Any, exception_traceback: Any
    ) -> None:
        """Exit the runtime context for the SnowflakeClient object.

        This method is called upon exiting the 'with' statement and it triggers the
        `stop` method to close the connection and clean up resources.

        :param exception_type: The type of the exception, if raised in the 'with' block.
        :param exception_value: The exception value, if raised.
        :param exception_traceback: The traceback of the exception, if raised.
        """
        self.stop()

    def connect_to_schema(self, schema: str) -> Any:
        """Switch the current Snowflake schema to the specified one.

        This method changes the active schema in the Snowflake session to the given schema.

        :param schema: The name of the schema to switch to.
        """
        self._sf.cursor().execute(f"USE SCHEMA {schema}")

    @property
    def ibis(self) -> ibis.BaseBackend:
        """Get the Ibis connection.

        This property provides access to the Ibis connection associated with the
        SnowflakeClient. It can be used to perform Ibis-specific operations.

        :return: The Ibis connection associated with this SnowflakeClient.
        """
        return self._ibis

    def query_to_ibis(self, query: str) -> IbisTable:
        """Execute a SQL query and return the result as an IbisTable object.

        This method executes a given SQL query against Snowflake and returns the result
        as an IbisTable, which can be used for further operations in Ibis.

        :param query: The SQL query string to execute.
        :return: An IbisTable containing the results of the query.
        """
        return IbisTable(self.ibis.sql(query))

    def execute_to_df(
        self, query: str, columns: Optional[Schema] = None
    ) -> LocalDataFrame:
        """Execute a SQL query and return the result as a DataFrame.

        This method executes a SQL query in Snowflake and returns the result as a
        LocalDataFrame. Optionally, a schema can be provided to define the structure
        of the returned DataFrame.

        :param query: The SQL query string to execute.
        :param columns: Optional schema to apply to the resulting DataFrame.
        :return: A LocalDataFrame containing the query results.
        """
        cursor = self._sf.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        if columns is None:
            cols = cursor.description
            pa_schema = pa.schema([(c[0], FIELD_TYPES[c[1]].pa_type()) for c in cols])
            columns = Schema(pa_schema)
        return ArrayDataFrame(rows, columns)

    def load_df(self, df: DataFrame, name: str, mode: str = "overwrite") -> None:
        """Load a DataFrame into a Snowflake table.

        This method loads a DataFrame into Snowflake, creating or appending to a table.
        The mode determines whether the data should overwrite or append to the existing table.

        :param df: The DataFrame to load into Snowflake.
        :param name: The name of the Snowflake table.
        :param mode: Mode of loading the DataFrame - 'overwrite' or 'append'.
        :raises ValueError: If the mode is not supported.
        """
        if isinstance(df, ArrayDataFrame):
            df_pandas = df.as_pandas()
        else:
            df_pandas = ArrowDataFrame(df).as_pandas()

        if mode == "overwrite":
            snowflake.connector.pandas_tools.write_pandas(
                self.sf, df_pandas, name, overwrite=True
            )
        elif mode == "append":
            snowflake.connector.pandas_tools.write_pandas(self.sf, df_pandas, name)
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    def save_df(
        self,
        path: str,
        mode: str = "overwrite",
        **kwargs: Any,
    ) -> Callable[[DataFrame], None]:
        """Create a function to save a DataFrame to a specified path in Snowflake.

        This method returns a function that can be used to save a DataFrame to a given
        path in Snowflake, with the specified mode of operation.

        :param path: The path where the DataFrame will be saved in Snowflake.
        :param mode: Mode of saving the DataFrame - 'overwrite' or other supported modes.
        :return: A function that takes a DataFrame as an argument and saves it to Snowflake.
        """

        def _save(df: DataFrame) -> None:
            self.load_df(df, path, mode=mode, **kwargs)

        return _save

    def create_temp_table(self, schema: Schema | pa.Schema) -> str:
        """Create a temporary table in Snowflake with a given schema.

        This method creates a new temporary table in Snowflake with the specified schema
        and returns the name of the created table.

        :param schema: The schema to use for creating the temporary table.
        :return: The name of the created temporary table.
        """
        temp_table_name = f"temp_{uuid4().hex}".upper()
        if isinstance(schema, pa.Schema):
            pass
        elif isinstance(schema, Schema):
            schema = schema.pa_schema
        else:
            raise TypeError(f"Unsupported schema type: {type(schema)}")

        # CURRENTLY CONVERTING TO IBIS SCHEMA DOESN'T WORK - METHOD REQUIRES `ibis-framework>=7.2.0`
        # ibis_schema = PyArrowSchema.to_ibis(schema)

        # self.ibis.create_table(temp_table_name, schema=ibis_schema, temp=True)

        self._temp_tables.append(temp_table_name)

        return temp_table_name

    def register_temp_table(self, name: str):
        """Register a temporary table name within the SnowflakeClient.

        This method keeps track of a temporary table by adding its name to the list of
        managed temporary tables. It's useful for managing the lifecycle of temporary tables.

        :param name: The name of the temporary table to register.
        """
        self._temp_tables.append(name)

    def is_temp_table(self, name: str) -> bool:
        """Check if a table name is a registered temporary table.

        This method determines whether a given table name corresponds to a temporary table
        managed by this SnowflakeClient instance.

        :param name: The name of the table to check.
        :return: True if the table is a registered temporary table, False otherwise.
        """
        return name in self._temp_tables

    def df_to_table(
        self, df: AnyDataFrame, table_name: str = None, overwrite: bool = False
    ) -> Any:
        """Convert a DataFrame to a Snowflake table.

        This method converts a given DataFrame to a Snowflake table. If no table name is
        provided, a temporary table is created. The method supports overwriting or appending
        to an existing table.

        :param df: The DataFrame to convert.
        :param table_name: Optional name for the Snowflake table.
        :param overwrite: Whether to overwrite an existing table, defaults to False.
        :return: The name of the Snowflake table.
        """
        if table_name is None:
            if isinstance(df, ArrayDataFrame):
                schema = pa.Table.from_pandas(df.as_pandas()).schema
            else:
                schema = ArrowDataFrame(df).schema
            table_name = self.create_temp_table(schema)

        self.load_df(df, table_name, mode="overwrite" if overwrite else "append")

        return table_name

    def arrow_to_table(self, ptable: pa.Table, table: Optional[str] = None) -> str:
        """Load a PyArrow table into Snowflake.

        This method loads a PyArrow table into Snowflake, creating a temporary or specified
        table. The data is first written to a Parquet file before being loaded into Snowflake.

        :param ptable: The PyArrow table to load.
        :param table: Optional name of the Snowflake table to create or update.
        :return: The name of the Snowflake table where the data is loaded.
        """
        tb = table or self.create_temp_table(ptable.schema)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tf:
            temp_file_name = tf.name
        try:
            pq.write_table(ptable, temp_file_name)
            stage_name = f"{tb}_STAGE"
            self.ibis.raw_sql(f"CREATE TEMP STAGE {stage_name}").close()
            self.ibis.raw_sql(f"PUT file://{temp_file_name} @{stage_name}").close()
            self.ibis.raw_sql(
                f"""
                COPY INTO {tb} 
                FROM @{stage_name} 
                FILE_FORMAT = (TYPE = 'PARQUET') 
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                """
            ).close()
        finally:
            os.remove(temp_file_name)
        return tb
