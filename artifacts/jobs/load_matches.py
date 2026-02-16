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
    DATASET,
)

MATCHES_TABLE = f"{DATASET}.fact_matches"
TOURNAMENTS_TABLE = f"{DATASET}.dim_tournaments"

MATCHES_SCHEMA = T.StructType([
    T.StructField("tourney_id",      T.StringType(),  nullable = False),
    T.StructField("tourney_name",    T.StringType(),  nullable = True),
    T.StructField("surface",         T.StringType(),  nullable = True),
    T.StructField("draw_size",       T.IntegerType(), nullable=True),
    T.StructField("tourney_level",   T.StringType(),  nullable=True),
    T.StructField("tourney_date",    T.StringType(),  nullable=True),
    T.StructField("match_num",       T.IntegerType(), nullable=True),
    T.StructField("winner_id",       T.IntegerType(), nullable=True),
    T.StructField("winner_seed",     T.IntegerType(), nullable=True),
    T.StructField("winner_entry",    T.StringType(),  nullable=True),
    T.StructField("winner_name",     T.StringType(),  nullable=True),
    T.StructField("winner_hand",     T.StringType(),  nullable=True),
    T.StructField("winner_ht",       T.IntegerType(), nullable=True),
    T.StructField("winner_ioc",      T.StringType(),  nullable=True),
    T.StructField("winner_age",      T.FloatType(),   nullable=True),
    T.StructField("loser_id",        T.IntegerType(), nullable=True),
    T.StructField("loser_seed",      T.IntegerType(), nullable=True),
    T.StructField("loser_entry",     T.StringType(),  nullable=True),
    T.StructField("loser_name",      T.StringType(),  nullable=True),
    T.StructField("loser_hand",      T.StringType(),  nullable=True),
    T.StructField("loser_ht",        T.IntegerType(), nullable=True),
    T.StructField("loser_ioc",       T.StringType(),  nullable=True),
    T.StructField("loser_age",       T.FloatType(),   nullable=True),
    T.StructField("score",           T.StringType(),  nullable=True),
    T.StructField("best_of",         T.IntegerType(), nullable=True),
    T.StructField("round",           T.StringType(),  nullable=True),
    T.StructField("minutes",         T.IntegerType(), nullable=True),
    # ── winner serve stats ──
    T.StructField("w_ace",           T.IntegerType(), nullable=True),
    T.StructField("w_df",            T.IntegerType(), nullable=True),
    T.StructField("w_svpt",          T.IntegerType(), nullable=True),
    T.StructField("w_1stIn",         T.IntegerType(), nullable=True),
    T.StructField("w_1stWon",        T.IntegerType(), nullable=True),
    T.StructField("w_2ndWon",        T.IntegerType(), nullable=True),
    T.StructField("w_SvGms",         T.IntegerType(), nullable=True),
    T.StructField("w_bpSaved",       T.IntegerType(), nullable=True),
    T.StructField("w_bpFaced",       T.IntegerType(), nullable=True),
    # ── loser serve stats ──
    T.StructField("l_ace",           T.IntegerType(), nullable=True),
    T.StructField("l_df",            T.IntegerType(), nullable=True),
    T.StructField("l_svpt",          T.IntegerType(), nullable=True),
    T.StructField("l_1stIn",         T.IntegerType(), nullable=True),
    T.StructField("l_1stWon",        T.IntegerType(), nullable=True),
    T.StructField("l_2ndWon",        T.IntegerType(), nullable=True),
    T.StructField("l_SvGms",         T.IntegerType(), nullable=True),
    T.StructField("l_bpSaved",       T.IntegerType(), nullable=True),
    T.StructField("l_bpFaced",       T.IntegerType(), nullable=True),
    # ── ranking at time of match ──
    T.StructField("winner_rank",        T.IntegerType(), nullable=True),
    T.StructField("winner_rank_points", T.IntegerType(), nullable=True),
    T.StructField("loser_rank",         T.IntegerType(), nullable=True),
    T.StructField("loser_rank_points",  T.IntegerType(), nullable=True),
])

# transform: fact_matches
def transform_matches(df):
    """
    process match CSV into fact_matches schema
    """
    df = trim_string_columns(df)
    df = nullify_empty_strings(df)

    # parse tourney date
    df = df.withColumn(
        "tourney_date",
        F.to_date(F.col("tourney_date"), "yyyyMMdd")
    )

    # capitalize round codes
    df = df.withColumn("round", F.upper(F.col("round")))

    # make (best-of) X more descriptive
    df = df.withColumn(
        "best_of",
        F.concat(F.lit("BO"), F.col("best_of").cast(T.StringType()))
    )

    # select fact_matches columns only (no matches_stats yet)
    df = df.select(
        "tourney_id",
        "match_num",
        "tourney_date",

        # foreign keys
        "winner_id",
        "loser_id",

        # match context
        "surface",
        "draw_size",
        "tourney_level",
        F.col("round").alias("match_round"),
        "best_of",

        # match result
        "score",
        F.col("minutes").alias("match_minutes"),

        # winner metadata at matchtime
        "winner_seed",
        "winner_entry",
        "winner_rank",
        "winner_rank_points",
        F.col("winner_age").cast(T.IntegerType()).alias("winner_age"),
        "winner_ht",
        "winner_hand",
        "winner_ioc",

        # loser metadata
        "loser_seed",
        "loser_entry",
        "loser_rank",
        "loser_rank_points",
        "loser_age",
        "loser_ht",
        "loser_hand",
        "loser_ioc",
    )

    return df

def audit_tournament_consistency(df):
    """
    detect inconsistencies in tournament attributes (match context) before collapsing.

    also detect inconsistencies in player attributes before collapsing.
    """

    inconsistencies = (
        df.groupBy("tourney_id")
        .agg(
            F.size(F.collect_set("tourney_name")).alias("distinct_names"),
            F.size(F.collect_set("surface")).alias("distinct_surfaces"),
            F.size(F.collect_set("tourney_level")).alias("distinct_levels"),
            F.size(F.collect_set("draw_size")).alias("distinct_draw_sizes"),

            F.collect_set("tourney_name").alias("name_values"),
            F.collect_set("surface").alias("surface_values"),
            F.collect_set("tourney_level").alias("level_values"),
            F.collect_set("draw_size").alias("draw_size_values"),

            F.count("*").alias("total_matches")
        )
        # note: intentionally do not check best-of format since this DOES vary at different stages
        .filter(
            # flag tournaments where at least one attribute has inconsistency
            (F.col("distinct_names") > 1) |
            (F.col("distinct_surfaces") > 1) |
            (F.col("distinct_levels") > 1) |
            (F.col("distinct_draw_sizes") > 1)
        )


    )
    # log findings
    inconsistency_count = inconsistencies.count()
    if inconsistency_count > 0:
        print(f"found {inconsistency_count} tournaments with attributes inconsistencies")
        inconsistencies.select(
            "tourney_id", "total_matches",
            "distinct_names", "name_values",
            "distinct_surfaces", "surface_values",
            "distinct_levels", 'level_values',
            "distinct_draw_sizes", "draw_size_values"
        ).show(50, truncate=80)
    else:
        print("no inconsistencies detected in matches")

    return inconsistencies

# NOTE: no player audit but might add later


def derive_tournaments(df):
    """
    derive tournament dimension from match data.
    applies 'mode' via window function to settle on a value./
    """

    # group attributes to preserve actual combinations
    # should amount to same thing if data behaves decently
    tourney_attrs = (
        df.groupBy("tourney_id", "tourney_name", "surface", "tourney_level", "draw_size")
        .agg(
            F.count("*").alias("match_count"),
            F.min("tourney_date").alias("first_seen_date"),
            F.max("tourney_date").alias("last_seen_date")
        )
    )

    # rank each combination by frequency, per tournament
    window = Window.partitionBy("tourney_id").orderBy(F.desc("match_count"))
    tourney_ranked = tourney_attrs.withColumn("rn", F.row_number().over(window))

    # get date ranges for whole tourney so that 'first_seen_date' and 'last_seen_date' extend
    # to whole tournament and not just the time range subset of the combination.
    # Otherwise might not include all dates due to those dates falling on mislabelled data.
    date_ranges = (
        df.groupBy("tourney_id")
        .agg(
            F.min("tourney_date").alias("first_seen_date"),
            F.max("tourney_date").alias("last_seen_date")
        )
    )

    dim_tournaments = (
        tourney_ranked
        .filter(F.col("rn") == 1)
        .drop("rn", "match_count", "first_seen_date", "last_seen_date")
        .join(date_ranges, on="tourney_id", how = "left")
        .select(
            "tourney_id",
            "tourney_name",
            "tourney_level",
            "surface",
            "draw_size",
            "first_seen_date",
            "last_seen_date"
        )
    )

    return dim_tournaments

def main():
    parser = argparse.ArgumentParser(description = "load fact_matches + dim_tournaments to bigquery")
    parser.add_argument("--input_path", required = True,
        help = "GCS path to match CSVs directory or glob \
            (e.g. gs://bucket/bronze/.../dt=2025-02-01/atp_matches_*.csv)")
    args = parser.parse_args()

    spark = get_spark_session("load_matches")

    # extract
    raw_df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(MATCHES_SCHEMA)
        .csv(args.input_path)
    )

    total_rows = raw_df.count()
    print(f"Read {total_rows} match rows from {args.input_path}")

    # transform fact_matches
    matches_df = transform_matches(raw_df)

    source_label = "atp_matches_*.csv"
    matches_final = add_audit_columns(matches_df, source_label)

    # transform dim_tournaments
    print("\nApply tournament consistency audit...")
    audit_tournament_consistency(matches_df)    # output audit to LOG table later

    tournaments_df = derive_tournaments(matches_df)
    tournaments_final = add_audit_columns(tournaments_df, source_label)


    # load dim_tournaments first for correct data lineage
    tourney_count = tournaments_final.count()
    print(f"Writing {tourney_count} tournaments to {TOURNAMENTS_TABLE}...")
    write_to_bigquery(tournaments_final, TOURNAMENTS_TABLE, mode="overwrite")

    match_count = matches_final.count()
    print(f"Writing {match_count} matches to {MATCHES_TABLE}...")
    write_to_bigquery(matches_final, MATCHES_TABLE, mode="overwrite")

    print("Done.")
    spark.stop()

if __name__ == "__main__":
    main()