from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


# constants
PROJECT_ID = "tennis-etl-pipeline"
DATASET = "tennis_raw"
TEMP_GCS_BUCKET = "tennis-etl-bucket"

# sparksession builder
def get_spark_session(app_name: str) -> SparkSession:
    """
    Build SparkSession pre-configured for BQ writes.
    stages data as Parquet before bulk-loading into BQ.
    sets up GCP parent project.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .config("parentProject", PROJECT_ID)
        .getOrCreate()
    )


# common cleaning functions
def trim_string_columns(df: DataFrame) -> DataFrame:
    """
    trim leading & trailing whitespaces from all string columns
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))

    return df


def nullify_empty_strings(df: DataFrame) -> DataFrame:
    """
    convert empty strings to NULL values
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            df = df.withColumn(
                field.name, 
                F.when(F.col(field.name) == "", None).otherwise(F.col(F.col(field.name)))
            )

    return df


def add_audit_columns(df: DataFrame, source_file: str) -> DataFrame:
    """
    append source_file, loaded_at, and updated_at columns for auditing
    """
    return (
        df.withColumn("source_file", F.lit(source_file))
        .withColumn("loaded_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )


def write_to_bigquery(df: DataFrame, table: str, mode: str = 'overwrite'):
    """
    write df to BQ via spark-bigquery connector
    """

    (
        df.write
        .format("bigquery")
        .option("table", f"{PROJECT_ID}.{table}")
        .option("writeMethod", "direct")
        .mode(mode)
        .save()
    )