"""
etl_utils - shared utilities for tennis ETL PySpark jobs.

Package distributed as .whl and installed on DataProc Serverless
workers so every job utilizes shared functionality:

    from etl_utils import get_spark_session, write_to_bigquerr, DATASET

"""

from etl_utils._core import (
    # constants
    PROJECT_ID,
    DATASET,
    TEMP_GCS_BUCKET,

    # session management
    get_spark_session,

    # cleaning functions
    trim_string_columns,
    nullify_empty_strings,

    # audit
    add_audit_columns,

    # I/O
    write_to_bigquery
)

__all__ = [
    "PROJECT_ID",
    "DATASET",
    "TEMP_GCS_BUCKET",
    "get_spark_session",
    "trim_string_columns",
    "nullify_empty_strings",
    "add_audit_columns",
    "write_to_bigquery"
]

__version__ = "0.1.0"