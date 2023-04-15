import datetime
import json
import os
import tempfile
from contextvars import ContextVar
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from uuid import uuid4

import ibis
import pandas as pd
import pandas_gbq
import pyarrow as pa
import pyarrow.parquet as pq
from fugue import (
    ArrayDataFrame,
    ArrowDataFrame,
    DataFrame,
    ExecutionEngine,
    LocalDataFrame,
    PartitionSpec,
)
from fugue_ibis import IbisTable
from fugue_warehouses import WarehouseClientBase
from google.api_core import client_info as rest_client_info
from google.api_core.gapic_v1 import client_info as grpc_client_info
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery.table import Table as BQTable
from google.oauth2 import service_account
from triad import ParamDict, Schema, SerializableRLock, assert_or_throw
from triad.utils.convert import to_timedelta
from triad.utils.schema import quote_name, safe_split_out_of_quote, unquote_name

import fugue_warehouses_version

from ._constants import FUGUE_BQ_CONF_CREDENTIALS_ENV, FUGUE_BQ_CONF_PROJECT
from ._utils import get_temp_dataset, normalize_pa_schema, normalize_pa_table

_FUGUE_BIGQUERY_CLIENT_CONTEXT = ContextVar(
    "_FUGUE_BIGQUERY_CLIENT_CONTEXT", default=None
)

_CONTEXT_LOCK = SerializableRLock()


class BigQueryClient(WarehouseClientBase):
    @staticmethod
    def get_or_create(conf: Any = None) -> "BigQueryClient":
        with _CONTEXT_LOCK:
            res = _FUGUE_BIGQUERY_CLIENT_CONTEXT.get()
            if res is None:
                _conf = ParamDict(conf)
                tds = get_temp_dataset(_conf)
                project = _conf.get_or_none(FUGUE_BQ_CONF_PROJECT, str)
                ce = _conf.get_or_none(FUGUE_BQ_CONF_CREDENTIALS_ENV, str)
                if ce is not None:
                    info = json.loads(os.environ[ce])
                    credentials_func: Any = (
                        lambda: service_account.Credentials.from_service_account_info(
                            info
                        )
                    )
                else:
                    credentials_func = None
                res = BigQueryClient(
                    temp_dataset=tds, project=project, credentials_func=credentials_func
                )
                _FUGUE_BIGQUERY_CLIENT_CONTEXT.set(res)  # type: ignore
            return res

    @staticmethod
    def get_current() -> "BigQueryClient":
        with _CONTEXT_LOCK:
            res = _FUGUE_BIGQUERY_CLIENT_CONTEXT.get()
            assert_or_throw(
                res is not None, ValueError("no BigQueryClient was initialized")
            )
            return res  # type: ignore

    def __init__(
        self,
        temp_dataset: str,
        project: Optional[str] = None,
        credentials_func: Optional[
            Callable[[], Optional[service_account.Credentials]]
        ] = None,
    ):
        self._temp_dataset = temp_dataset
        self._temp_tables: List[str] = []
        self._default_project = project
        self._bq_client_info = rest_client_info.ClientInfo(
            user_agent=f"fugue-bigquery/{fugue_warehouses_version.__version__}"
        )
        self._bqstorage_client_info = grpc_client_info.ClientInfo(
            user_agent=f"fugue-bigquery/{fugue_warehouses_version.__version__}"
        )
        self._credentials_func = (
            credentials_func if credentials_func is not None else lambda: None
        )
        credentials = self._credentials_func()
        self._bq = bigquery.Client(
            project=self._default_project,
            client_info=self._bq_client_info,
            credentials=credentials,
        )
        self._bqs = bigquery_storage.BigQueryReadClient(
            credentials=self._bq._credentials,
            client_info=self._bqstorage_client_info,
        )
        self._ibis = ibis.bigquery.connect(
            project_id=self.project,
            dataset_id=self.project + "." + self._temp_dataset,
            credentials=self._bq._credentials,
        )
        self._src_ibis_tables: Dict[int, Tuple[IbisTable, BQTable]] = {}
        pandas_gbq.context.credentials = self.bq._credentials
        pandas_gbq.context.project = self.bq.project

    @property
    def bq(self) -> bigquery.Client:
        return self._bq

    @property
    def project(self) -> str:
        return self.bq.project

    def table_to_full_name(self, name: str) -> str:
        s = name.split(".")
        if len(s) == 1:
            return self.project + "." + self._temp_dataset + "." + name
        if len(s) == 2:
            return self.project + "." + name
        return name

    @property
    def bqs(self) -> bigquery_storage.BigQueryReadClient:
        return self._bqs

    @property
    def ibis(self) -> ibis.BaseBackend:
        return self._ibis

    def stop(self):
        for tt in self._temp_tables:
            self.bq.delete_table(tt, not_found_ok=True)
        self.bqs.transport.grpc_channel.close()
        self.bq.close()
        self.ibis.client.close()  # type: ignore

    def __enter__(self) -> "BigQueryClient":
        return self

    def __exit__(
        self, exception_type: Any, exception_value: Any, exception_traceback: Any
    ) -> None:
        self.stop()

    def arrow_to_table(
        self, ptable: pa.Table, table: Optional[str] = None, expiration: Any = None
    ) -> str:
        tb = table or self._generate_temp_table()
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tf:
            pq.write_table(ptable, tf.name)
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE",
            )
            with open(tf.name, "rb") as f:
                load_job = self.bq.load_table_from_file(f, tb, job_config=job_config)
                load_job.result()
        if expiration is not None:
            exp = datetime.datetime.now() + to_timedelta(expiration)
            table_ref = self.bq.get_table(tb)
            table_ref.expires = exp
            self.bq.update_table(table_ref, ["expires"])
        return tb

    def query_to_pd(self, query: str) -> pd.DataFrame:
        return pandas_gbq.read_gbq(query, progress_bar_type=None)

    def query_to_table(
        self,
        query: str,
        table: Optional[str] = None,
        expiration: Any = None,
        is_view: bool = False,
    ) -> str:
        tb = unquote_name(table) if table is not None else self._generate_temp_table()
        if expiration is None:
            exp = ""
        else:
            sec = int(to_timedelta(expiration).total_seconds())
            exp = (
                "expiration_timestamp="
                f"TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL {sec} SECOND)"
            )
        tv = "TABLE" if not is_view else "VIEW"
        sql = (
            f"""
CREATE OR REPLACE {tv} {quote_name(tb)}
OPTIONS(
    {exp}
) AS"""
            + "\n"
            + query
        )
        # print(sql)
        self.bq.query(sql).result()
        return tb

    def table_to_ibis(
        self, table: str, columns: Optional[List[str]] = None
    ) -> IbisTable:
        tb = self.table_to_full_name(table)
        parts = safe_split_out_of_quote(tb, ".")
        idf: IbisTable = self.ibis.table(parts[2], database=parts[0] + "." + parts[1])
        if columns is not None:
            idf = idf[columns]
        table_ref = self.bq.get_table(tb)
        self._src_ibis_tables[id(idf)] = (idf, table_ref)
        return idf

    def get_src_ref(self, table: IbisTable) -> Optional[BQTable]:
        key = id(table)
        if key in self._src_ibis_tables:
            return self._src_ibis_tables[key][1]
        return None

    def read_table(
        self,
        engine: ExecutionEngine,
        table: str,
        row_filter: Optional[str] = None,
        columns: Any = None,
        max_stream_count: int = 0,
        read_kwargs: dict = None,
    ) -> DataFrame:
        _read_kwargs: Dict[str, Any] = read_kwargs or {}
        project = self.project

        table_ref = self.bq.get_table(table)
        if table_ref.table_type == "VIEW":
            raise TypeError("Table type VIEW not supported")

        def make_create_read_session_request():
            return bigquery_storage.types.CreateReadSessionRequest(
                max_stream_count=max_stream_count,
                # preferred_min_stream_count=2,
                parent=f"projects/{project}",
                read_session=bigquery_storage.types.ReadSession(
                    data_format=bigquery_storage.types.DataFormat.ARROW,
                    read_options=bigquery_storage.types.ReadSession.TableReadOptions(
                        row_restriction=row_filter, selected_fields=columns
                    ),
                    table=table_ref.to_bqstorage(),
                ),
            )

        # Create a read session in order to detect the schema.
        # Read sessions are light weight and will be auto-deleted after 24 hours.
        session = self.bqs.create_read_session(make_create_read_session_request())
        schema = pa.ipc.read_schema(
            pa.py_buffer(session.arrow_schema.serialized_schema)
        )
        schema = normalize_pa_schema(schema)

        streams = ArrayDataFrame(
            [
                [stream.name, self.project, self._temp_dataset]
                for stream in session.streams
            ],
            "name:str,project:str,temp_dataset:str",
        )
        credentials_func = self._credentials_func

        def _map(cursor: Any, df: LocalDataFrame) -> LocalDataFrame:
            tbs: List[pa.Table] = []
            for row in df.as_dict_iterable():
                tbs.append(
                    _read_batch(
                        make_create_read_session_request,
                        row["temp_dataset"],
                        row["project"],
                        _read_kwargs,
                        row["name"],
                        credentials_func=credentials_func,
                    )
                )
            res = normalize_pa_table(pa.concat_tables(tbs))
            return ArrowDataFrame(res)

        res = engine.map_engine.map_dataframe(
            streams,
            _map,
            output_schema=Schema(schema),
            partition_spec=PartitionSpec("per_row"),
        )
        return res

    def _generate_temp_table(self) -> str:
        name = self.table_to_full_name("temp_" + str(uuid4())[:7])
        self._temp_tables.append(name)
        return name


def _read_batch(
    make_create_read_session_request: Callable,
    temp_dataset: str,
    project: str,
    read_kwargs: Dict[str, Any],
    stream_name: str,
    credentials_func: Any,
) -> pa.Table:
    def _get_batch_records(
        bqs_client: bigquery_storage.BigQueryReadClient,
    ) -> Iterable[Any]:
        for message in bqs_client.read_rows(name=stream_name, offset=0, **read_kwargs):
            yield pa.ipc.read_record_batch(
                pa.py_buffer(message.arrow_record_batch.serialized_record_batch),
                schema,
            )

    with BigQueryClient(
        temp_dataset, project, credentials_func=credentials_func
    ) as client:
        session = client.bqs.create_read_session(make_create_read_session_request())
        schema = pa.ipc.read_schema(
            pa.py_buffer(session.arrow_schema.serialized_schema)
        )
        return pa.Table.from_batches(_get_batch_records(client.bqs), schema=schema)
