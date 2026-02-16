from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import argparse

from etl_utils import (
    get_spark_session,
    trim_string_columns,
    nullify_empty_strings,
    add_audit_columns,
    write_to_bigquery,
    DATASET
)

TARGET_TABLE = f"{DATASET}.fact_rankings"

RANKINGS_SCHEMA = T.StructType([
    T.StructField("ranking_date",       T.StringType(),     nullable = False),
    T.StructField("rank",               T.IntegerType(),    nullable = False),
    T.StructField("player",             T.StringType(),      nullable = False),
    T.StructField("points",             T.IntegerType(),    nullable = False)
])

def transform_rankings(df):
    """
    transform raw ranking CSV data to match fact_rankings BQ schema
    """
    df = trim_string_columns(df)
    df = nullify_empty_strings(df)

    # parse date strings
    df = df.withColumn(
        'ranking_date',
        F.to_date(F.col("ranking_date"), "yyyyMMdd")
    )

    # rename player column to player_id
    df = df.withColumnRenamed("player", "player_id")

    # compute new ranking_sequence col (for issues when a player has two rankings on a given day)
    window = Window.partitionBy("ranking_date", "player_id").orderBy(F.asc("rank"))
    df = df.withColumn(
        'ranking_sequence',
        F.row_number().over(window)
    )

    # validate rank values
    invalid_ranks = df.filter(F.col("rank") <= 0)
    invalid_count = invalid_ranks.count()
    if invalid_count > 0:
        print(f"Found {invalid_count} rows with invalid (<= 0) rank values")
        invalid_ranks.show(20, truncate = False)

    # select final columns
    df = df.select(
        "ranking_date",
        "player_id",
        "rank",
        "points",
        "ranking_sequence"
    )
    df.orderBy("ranking_date", "rank")
    return df

def audit_ranking_quality(df):

    """
    run quality checks for ranking snapshot data.
    validate properties for well-behaved ranking data
    """

    # check duplicate (date, player_id) pairs
    dupes = (
        df.groupBy("ranking_date", "player_id")
        .agg(F.count("*").alias("occurrences"))
        .filter(F.col("occurrences") > 1)
    )

    dupe_count = dupes.count()
    if dupe_count > 0:
        print(f"Found {dupe_count} duplicate 9date, player) pairs:")
        dupes.orderBy(F.desc("occurrences")).show(20, truncate = False)
    else:
        print("No duplicate (date, player) pairs found.")

    
    # check rank gaps within a snapshot
    snapshot_stats = (
        df.groupBy("ranking_date")
        .agg(
            F.count("*").alias("player_count"),
            F.max("rank").alias("max_rank"),
            F.min("rank").alias("min_rank")
        )
        .withColumn("gap_ratio", F.col("max_rank") / F.col("player_count"))
    )
    # gap_ratio is a surface measure of coverage (inf, 1) range

    gappy_snapshots = snapshot_stats.filter(F.col("gap_ratio") > 1.5)
    gappy_count = gappy_snapshots.count()
    if gappy_count > 0:
        print(f"Found {gappy_count} ranking-date snapshots with significant rank gaps\
              (max_rank > 1.5x player_count).")
        gappy_snapshots.orderBy("ranking_date").show(20, truncate = False)

    else:
        print("All ranking snapshots have reasonably dense ranking snapshots.")

    
    # check monotonicity of points
    top_two = (
        df.filter(F.col("rank").isin(1, 2))     # filter early
        .groupBy("ranking_date")                # group by snapshot-date
        .pivot("rank", [1,2])                   # 'pivot' takes values and turns them into column headers
        .agg(F.first("points"))                 # 'agg' regates the entries that may have also mapped (rank == 1|2)
        .withColumnRenamed("1", "rank1_points")
        .withColumnRenamed("2", "rank2_points")
        .filter(
            F.col("rank1_points").isNotNull() &
            F.col("rank2_points").isNotNull()
        )
    )

    inversions = top_two.filter(F.col("rank1_points") < F.col("rank2_points"))
    inversion_count = inversions.count()
    if inversion_count > 0:
        print(f"Found {inversion_count} dates where #1 has fewer points than #2")
        inversions.show(10, truncate = False)
    else:
        print(f"Points ordering is consistent")

    return {
        "duplicates": dupes,
        "gappy_snapshots": gappy_snapshots,
        "point_inversions": inversions,
    }

# entry point
def main():
    parser = argparse.ArgumentParser(description = "load fact_rankings to BigQuery")
    parser.add_argument("--input_path", required = True, 
                        help = "GCS path to rankings CSVs \
                            (e.g. gs://bucket/bronze/.../dt=2025-02-01/atp_rankings_*.csv)")
    args = parser.parse_args()

    spark = get_spark_session("load_rankings")

    # extract
    raw_df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(RANKINGS_SCHEMA)
        .csv(args.input_path)
    )

    total_rows = raw_df.count()
    print(f"Read {total_rows} ranking rows from {args.input_path}")

    # deduplicate
    deduped_df = raw_df.dropDuplicates()
    deduped_count = deduped_df.count()
    dropped = total_rows - deduped_count
    if dropped > 0:
        print(f"Dropped {dropped} exact dupliates from overlapping files")

    # audit
    print("\nPerforming data quality audit...")
    audit_ranking_quality(deduped_df)

    # transform
    rankings_df = transform_rankings(deduped_df)
    source_label = "atp_rankings_*.csv"
    rankings_final = add_audit_columns(rankings_df, source_label)

    # load to bigquery
    final_count = rankings_final.count()
    print(f"\nWriting {final_count} rows to {TARGET_TABLE}")
    write_to_bigquery(rankings_final, TARGET_TABLE, mode = "overwrite")

    print("Completed.")
    spark.stop()

if __name__ == "__main__":
    main()


