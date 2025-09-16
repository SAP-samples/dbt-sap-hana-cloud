from contextlib import contextmanager
from dataclasses import dataclass
from dbt.adapters.contracts.connection import AdapterResponse, Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.exceptions import DbtRuntimeError
from hdbcli import dbapi  # Importing the correct library for SAP HANA
from dbt.adapters.events.logging import AdapterLogger
from time import time
from typing import Optional, Any, Tuple
from dbt.adapters.saphanacloud.connection_helper import SapHanaCloudConnection
from dbt_common.events.functions import fire_event
from dbt.adapters.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt_common.events.contextvars import get_node_info
from dbt_common.utils import cast_to_str
import os
import json

logger = AdapterLogger("saphanacloud")

DATATYPES = {
    0: "NULL",
    1: "TINYINT",
    2: "SMALLINT",
    3: "INTEGER",
    4: "BIGINT",
    5: "DECIMAL",
    6: "REAL",
    7: "DOUBLE",
    8: "CHAR",
    9: "VARCHAR",
    10: "NCHAR",
    11: "NVARCHAR",
    12: "BINARY",
    13: "VARBINARY",
    14: "DATE",
    15: "TIME",
    16: "TIMESTAMP",
    17: "TIME_TZ",
    18: "TIME_LTZ",
    19: "TIMESTAMP_TZ",
    20: "TIMESTAMP_LTZ",
    21: "INTERVAL_YM",
    22: "INTERVAL_DS",
    23: "ROWID",
    24: "UROWID",
    25: "CLOB",
    26: "NCLOB",
    27: "BLOB",
    28: "BOOLEAN",
    29: "STRING",
    30: "NSTRING",
    31: "BLOCATOR",
    32: "NLOCATOR",
    33: "BSTRING",
    34: "DECIMAL_DIGIT_ARRAY",
    35: "VARCHAR2",
    36: "VARCHAR3",
    37: "NVARCHAR3",
    38: "VARBINARY3",
    39: "VARGROUP",
    40: "TINYINT_NOTNULL",
    41: "SMALLINT_NOTNULL",
    42: "INT_NOTNULL",
    43: "BIGINT_NOTNULL",
    44: "ARGUMENT",
    45: "TABLE",
    46: "CURSOR",
    47: "SMALLDECIMAL",
    48: "ABAPITAB",
    49: "ABAPSTRUCT",
    50: "ARRAY",
    51: "TEXT",
    52: "SHORTTEXT",
    53: "FIXEDSTRING",
    54: "FIXEDPOINTDECIMAL",
    55: "ALPHANUM",
    56: "TLOCATOR",
    61: "LONGDATE",
    62: "SECONDDATE",
    63: "DAYDATE",
    64: "SECONDTIME",
    65: "CSDATE",
    66: "CSTIME",
    71: "BLOB_DISK",
    72: "CLOB_DISK",
    73: "NCLOB_DISK",
    74: "GEOMETRY",
    75: "POINT",
    76: "FIXED16",
    77: "BLOB_HYBRID",
    78: "CLOB_HYBRID",
    79: "NCLOB_HYBRID",
    80: "POINTZ"
}


@dataclass
class SapHanaCloudCredentials(Credentials):
    database: str
    host: Optional[str] = None
    port: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    cf_service_name: Optional[str] = None
    connect_timeout: int = 10

    def __post_init__(self) -> None:

        if self.cf_service_name is not None:

            cf_services = json.loads(os.environ['VCAP_SERVICES'])

            # Flatten all service entries into a single list
            services_flattend = [
                service
                for services in cf_services.values()
                for service in services
            ]

            hana_service = next(
                (service for service in services_flattend if service["name"] == self.cf_service_name), None)

            if hana_service is None:
                raise DbtRuntimeError(
                    "Non Cloud Foundry Service with the name available")

            self.schema = hana_service["credentials"].get("schema")
            self.user = hana_service["credentials"].get("user")
            self.password = hana_service["credentials"].get("password")
            self.host = hana_service["credentials"].get("host")
            self.port = hana_service["credentials"].get("port")

        if None in [self.schema, self.user, self.password, self.host, self.port]:
            raise DbtRuntimeError("One or more required credentials are None")

    _ALIASES = {"dbname": "database", "pass": "password"}

    @property
    def type(self):
        return "saphanacloud"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        return (
            "host",
            "port",
            "user",
            "database",
            "schema",
            "connect_timeout",
        )


class SapHanaCloudConnectionManager(SQLConnectionManager):
    TYPE = "saphanacloud"

    def __init__(self, profile, connections):
        super().__init__(profile, connections)
        self.in_auto_commit_mode = True

    def begin(self):
        pass

    def commit(self):
        if not self.in_auto_commit_mode:
            super().commit()

    def execute(self, sql, auto_begin=False, fetch=None, limit=None):
        if not self.in_auto_commit_mode and auto_begin:
            self.begin()

        # If a limit is provided, add it to the SQL query
        if limit:
            sql = f"{sql} LIMIT {limit}"

        # Execute the query with the possibility of fetching results
        return super().execute(sql, auto_begin, fetch=fetch)

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False
    ) -> Tuple[SapHanaCloudConnection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and not connection.transaction_open:
            self.begin()

        fire_event(
            ConnectionUsed(
                conn_type=self.TYPE,
                conn_name=cast_to_str(connection.name),
                node_info=get_node_info(),
            )
        )

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = f'{sql[:512]}...'
            else:
                log_sql = sql

            fire_event(
                SQLQuery(
                    conn_name=cast_to_str(connection.name),
                    sql=log_sql,
                    node_info=get_node_info()
                )
            )

            start_time = time()
            cursor = connection.handle.cursor()
            cursor.execute(sql, bindings)
            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round((time() - start_time)),
                    node_info=get_node_info(),
                )
            )
            return connection, cursor

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        # Customize this method to extract meaningful information from the SAP HANA cursor
        num_rows = 0
        activity = "success"
        message = "OK"
        try:
            if cursor is not None and cursor.rowcount is not None:
                num_rows = cursor.rowcount

            message = f"OK {num_rows}"

        except Exception as e:
            activity = "error"
            message = f"An error occurred: {str(e)}"

        return AdapterResponse(
            _message=message,
            rows_affected=num_rows,
            code=activity
        )

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except dbapi.Error as exc:
            logger.error(f"SAP HANA error: {str(exc)}")
            self.rollback_if_open()
            raise DbtRuntimeError(str(exc)) from exc
        except Exception as exc:
            logger.error(f"Error running SQL: {sql}")
            logger.info("Rolling back transaction.")
            self.rollback_if_open()
            raise DbtRuntimeError(str(exc)) from exc

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.info("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        try:
            handle = dbapi.connect(
                address=credentials.host,
                port=credentials.port,
                user=credentials.user,
                password=credentials.password,
                schema=credentials.schema
            )

            connection.handle = handle  # Set the connection handle

            cursor = connection.handle.cursor()
            try:
                logger.debug("Testing connection with 'SELECT 1 FROM DUMMY'")
                cursor.execute("SELECT 1 FROM DUMMY")
                connection.state = 'open'
                logger.debug("Connection successful")
            except Exception as exc:
                logger.error(f"Failed during connection test: {exc}")
                raise DbtRuntimeError("Connection test failed") from exc
            logger.debug("Connection successful finally")
            return connection

        except Exception as exc:
            logger.error(f"Error connecting to database: {exc}")
            connection.state = "fail"
            connection.handle = None
            raise DbtRuntimeError(str(exc)) from exc

    @classmethod
    def check_connection(cls, connection):
        logger.info("Overriding check_connection to use custom test.")
        return cls.test_connection(connection)

    @classmethod
    def test_connection(cls, connection):
        cursor = connection.handle.cursor()
        try:
            logger.info("from test_connection")
            cursor.execute("SELECT 1 FROM DUMMY")
            logger.info("Connection test passed.")
            return True
        except Exception as exc:
            logger.error(f"Connection test failed: {exc}")
            raise DbtRuntimeError("Connection test failed") from exc

    @classmethod
    def data_type_code_to_name(cls, type_code) -> str:
        try:
            return DATATYPES[type_code]
        except KeyError:
            print(f"Warning: Unknown type_code {type_code}")
            return f"unknown type_code {type_code}"

    def cancel(self, connection):
        """
        Implementation for canceling an ongoing query in SAP HANA.
        """
        logger.info("Canceling query")
        try:
            connection.handle.cancel()
            logger.info("Query successfully canceled.")
        except Exception as exc:
            logger.error(f"Failed to cancel query: {exc}")
            raise DbtRuntimeError("Failed to cancel query") from exc
