import argparse
from pyspark.sql import types as T
from pyspark.sql import functions as F

from etl_utils import (
    get_spark_session,
    trim_string_columns,
    nullify_empty_strings,
    add_audit_columns,
    write_to_bigquery,
    DATASET
)

TARGET_TABLE = f"{DATASET}.fact_match_stats"

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

# column mappings
STAT_COLUMN_MAP = [
    ("aces",            "w_ace",    "l_ace"),
    ("double_faults",   "w_df",     "l_df"),
    ("service_points",    "w_svpt",    "l_svpt"),
    ("first_serves_in",   "w_1stIn",   "l_1stIn"),
    ("first_serves_won",  "w_1stWon",  "l_1stWon"),
    ("second_serves_won", "w_2ndWon",  "l_2ndWon"),
    ("service_games",     "w_SvGms",   "l_SvGms"),
    ("break_points_saved","w_bpSaved", "l_bpSaved"),
    ("break_points_faced","w_bpFaced", "l_bpFaced"),
]


# transform logic
def unpivot_stats(df):
    """
    unpivot wide match data into tall per-player stat rows.
    """

    # match identity columns (same for both winner and loser rows)
    identity_cols = [
        F.col("tourney_id"),
        F.col("match_num"),
        F.col("tourney_date")
    ]

    # build winner row projections
    winner_cols = identity_cols + [
        F.col("winner_id").alias("player_id"),
        F.lit("winner").alias("player_role")
    ]

    # map each stat: w_ace -> aces, w_df -> double_faults, etc.
    for target_name, winner_col, _ in STAT_COLUMN_MAP:
        winner_cols.append(F.col(winner_col).alias(target_name))

    winner_stats = df.select(winner_cols)

    # build loser row projections
    loser_cols = identity_cols + [
        F.col("loser_id").alias("player_id"),
        F.lit("winner").alias("player_role")
    ]

    for target_name, _, loser_col in STAT_COLUMN_MAP:
        loser_cols.append(F.col(loser_col).alias(target_name))

    loser_stats = df.select(loser_cols)

    # combine loser and winner
    all_stats = winner_stats.unionByName(loser_stats)

    return all_stats


def drop_empty_stat_rows(df):
    """
    remove rows with no recorded stats.
    if there is any stat then we keep the row
    """

    stat_columns = [target for target, _, _ in STAT_COLUMN_MAP]

    # building the condition
    all_nulls_condition = F.lit(True)
    for col_name in stat_columns:

        # .isNull() returns a boolean mask assessing if col_name is null (1, M)
        # combine conditions using '&' to condense all 9 stat columns
        # '&' applies AND element-wise for two arrays
        all_nulls_condition = all_nulls_condition & F.col(col_name).isNull()

    return df.filter(~all_nulls_condition)


def validate_stat_ranges(df):
    """
    check that stat values make sense with respect to each other.
    aces, dfs >= 0
    service_pts > first_serves_in
    first_serves_in > first_serves_won
    brk_pts_faced > brk_pts_won
    """
    issues = []

    # check for non-negativities
    non_negative_cols = ["aces", "double_faults", "service_poinst",
                         "first_serves_in", "first_serves_won",
                         "second_serves_won", "service_games",
                         "break_points_saved", "break_points_faced"]
    for col_name in non_negative_cols:
        negative = df.filter(F.col(col_name) < 0)
        neg_count = negative.count()
        if neg_count > 0:
            issues.append(f"{col_name}:{neg_count}")


    # logical stat checks
    # service_points checks
    fsi_violations = df.filter(
        F.col("first_serves_in").isNotNull() &
        F.col("service_points").isNotNull() &
        F.col("first_serves_in") > F.col("service_points")
    )
    fsi_count = fsi_violations.count()
    if fsi_count > 0:
        issues.append(f"fsi > fs_pts violations: {fsi_count} times")

    # first-serves-in checks
    fsw_violations = df.filter(
        F.col("first_serves_won").isNotNull() & 
        F.col("first_serves_in").isNotNull() &
        F.col("first_serves_won") > F.col("first_serves_in")
    )
    fsw_count = fsw_violations.count()
    if fsw_count > 0:
        issues.append(f"fsw > fsi violations: {fsw_count} times")


    # break-points checks
    bp_violations = df.filter(
        F.col("break_points_saved").isNotNull() &
        F.col("break_points_faced").isNotNull() &
        F.col("break_points_saved") > F.col("break_points_faced")
    )
    bp_count = bp_violations.count()
    if bp_count > 0:
        issues.append(f"bp_saved > bp_faced violations: {bp_count} times")

    # summarize issues
    if issues:
        print(f"Found following stat violations:")
        for issue in issues:
            print(issue)
    else:
        print(f"All stat values are within valid ranges")


def main():
    parser = argparse.ArgumentParser(description = "load fact_match_stats to bigquery")
    parser.add_argument("--input_path", required = True,
                        help = "GCS path to match CSVs \
                            (e.g. gs://bucket/bronze/.../dt=2025-02-01/atp_matches_*.csv)")
    
    args = parser.parse_args()

    spark = get_spark_session("load_match_stats")

    raw_df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(MATCHES_SCHEMA)
        .csv(args.input_path)
    )

    total_rows = raw_df.count()
    print(f"Reading {total_rows} match rows from {args.input_path}")


    # clean data
    cleaned_df = trim_string_columns(raw_df)
    cleaned_df = nullify_empty_strings(raw_df)

    # date processing
    cleaned_df = cleaned_df.withColumn(
        "tourney_date",
        F.to_date(F.col("tourney_date"), "yyyyMMdd")
    )

    # transform by applying unpivot
    print(f"Applying unpivot to match stats")
    stats_df = unpivot_stats(cleaned_df)

    unpivot_count = stats_df.count()
    print(f"Unpivoted to {unpivot_count} rows (expected: ~{total_rows*2})")

    # filter rows with no recorded stats
    stats_df = drop_empty_stat_rows(stats_df)
    kept_count = stats_df.count()
    print(f"Dropped {unpivot_count - kept_count} rows, keeping {kept_count}")

    # validate domain checks
    validate_stat_ranges(stats_df)

    # add source tracking
    source_label = "atp_matches_*.csv"
    stats_final = add_audit_columns(stats_df, source_label)

    # load to bigquery
    print(f"\nWriting {kept_count} rows to {TARGET_TABLE}")
    write_to_bigquery(stats_final, TARGET_TABLE, mode = "overwrite")

    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()


