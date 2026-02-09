import argparse
from pyspark.sql import functions as F
from pyspark.sql import types as T

from etl_utils import (
    get_spark_session,
    trim_string_columns,
    nullify_empty_strings,
    add_audit_columns,
    write_to_bigquery,
    DATASET,
)

# table to edit
TARGET_TABLE = f"{DATASET}".dim_players



# Schema definition
PLAYERS_SCHEMA = T.StructType([
    T.StructField("player_id",      T.IntegerType(),    nullable = False),
    T.StructField("name_first",     T.StringType(),     nullable = True),
    T.StructField("name_last",      T.StringType(),     nullable = True),
    T.StructField("hand",           T.StringType(),     nullable = True),
    T.StructField("dob",            T.StringType(),     nullable = True),
    T.StructField("ioc",            T.StringType(),     nullable = True),
    T.StructField("height",         T.IntegerType(),    nullable = True),
    T.StructField("wikidata_id",    T.StringType(),     nullable = True)
])


# transformation logic
def transform_players(df):
    """
    transform raw CSV to match dim_players BQ schema
    """

    # common cleaning
    df = trim_string_columns(df)
    df = nullify_empty_strings(df)

    # parse date of birth
    df = df.withColumn(
        "dob",
        F.to_date(
            F.regexp_replace(F.col("dob"), r"\..*$", ""),        # remove decimal and everything trailing it
            "yyyyMMdd"      # parsing format
        )
    )

    # standardize handedness by converting exceptions to 'U'
    valid_hands = ["R", "L", "A", "U"]
    df = df.withColumn(
        "hand",
        F.when(F.upper(F.col("hand")).isin(valid_hands), F.upper(F.col("hand")))
        .otherwise(F.lit("U"))
    )

    # rename columns
    df = (
        df
        .withColumnRenamed("ioc", "country_code")
        .withColumnRenamed("height", "height_cm")
    )

    # compute full_name
    df = df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("name_first"), F.col("name_last"))
    )
    df = df.withColumn(
        "full_name",
        F.when(F.col("full_name") == "", None).otherwise(F.col("full_name"))        # convert to None if both first&last names are missing
    )

    # cast height to integer
    df = df.withColumn("height_cm", F.col("height_cm").cast(T.IntegerType()))

    # order and select final columns
    df = df.select(
        "player_id",
        "name_first",
        "name_last",
        "full_name",
        "hand",
        "dob",
        "country_code",
        "height_cm",
    )
    return df


# entry point
def main():

    # parse command-line arguments
    parser = argparse.ArgumentParser(description="load dim_players to BigQuery")
    parser.add_argument("--input_path", required = True, 
        help = "GCS path to atp_players.csv (e.g. gs://bucket/bronze/)")
    
    args = parser.parse_args()

    # initialize spark
    spark = get_spark_session("load_players")

    # extract
    raw_df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(PLAYERS_SCHEMA)
        .csv(args.input_path)
    )

    row_count = raw_df.count()
    print(f"Read {row_count} rows from {args.input_path}")

    # apply transformation
    cleaned_df = transform_players(raw_df)

    source_file = args.input_path.split("/")[-1]
    final_df = add_audit_columns(cleaned_df, source_file)

    # load
    print(f"Writing {final_df.count()} rows to {TARGET_TABLE}")
    write_to_bigquery(final_df, TARGET_TABLE, mode = 'overwrite')
    print("Done.")

    spark.stop()

if __name__ == "__main__":
    main()