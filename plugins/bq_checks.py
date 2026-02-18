
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from airflow.exceptions import AirflowFailException
import logging

log = logging.getLogger(__name__)


# config
PROJECT_ID = "tennis-etl-pipeline"
DATASET_ID = "tennis_raw"


EXPECTED_TABLES = [
    "dim_players",
    "dim_tournaments",
    "fact_matches",
    "fact_match_stats",
    "fact_rankings"
]


# set minimum expected row counts
MIN_ROW_COUNTS = {
    "dim_players":      1000,
    "dim_tournaments":  10,
    "fact_matches":     100,
    "fact_match_stats": 100,
    "fact_ranking":     1000,
}

# helper functions
def _get_client() -> bigquery.Client:
    return bigquery.Client(project = PROJECT_ID)

def _full_table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


# pre-flight checks
def preflight_schema_check(**context):
    """
    verify that bigquery dataset and all target tables exist BEFORE submitting Dataproc jobs.

    Raises AirflowFailException on missing tables to immediately fail the task.
    """

    client = _get_client()

    # check dataset exists
    try:
        client.get_dataset(DATASET_ID)
        log.info(f"Dataset '{DATASET_ID}' exists.")

    except NotFound:
        raise AirflowFailException(
            f"Dataset '{DATASET_ID}' not found in project '{PROJECT_ID}'.\n\
                Run: python scripst/manage_bigquery.py create"
        )
    

    missing = []
    for table_name in EXPECTED_TABLES:
        try:
            client.get_table(_full_table_id(table_name))
            log.info(f"{table_name} exists.")
        except NotFound:
            log.error(f"{table_name} NOT FOUND")
            missing.append(table_name)

    if missing:
        raise AirflowFailException(
            f"Missing BigQuery tables: {', '.join(missing)}\n\
                Run: python scripts/manage_bigquery.py recreate"
        )
    
    log.info("Schema check passed. All tables present")


# post-load validation
def postload_validation(**context):
    """
    Validate the following on Dataproc jobs completion:
        1. Every table has rows above the minimum threshold
        2. fact_match_stats has both 'winner' and 'loser'  player_role
        3. All tables were modified recently (within last 2 hours)

    Raises AirflowFailException on any failure.
    """
    client = _get_client()
    issues = []

    log.info("Running post-load validation...\n")

    # check 1: row counts
    log.info("Check 11: Min row count thresholds met")

    for table_name in EXPECTED_TABLES:
        try:
            table = client.get_table(_full_table_id(table_name))
            row_count = table.num_rows
            min_expected = MIN_ROW_COUNTS[table_name]

            # actual rows in table was smaller than expected
            if row_count < min_expected:
                msg = (
                    f"{table_name}: {row_count:,} rows \
                    (expected >= {min_expected:,})"
                )
                log.error(f"ERROR: {msg}")
                issues.append(msg)
            else:
                log.info(f"{table_name}: {row_count:,} rows")

        except NotFound:
            msg = f"{table_name}: table not found"
            log.error(f"ERROR: {msg}")
            issues.append(msg)

    # check 2: ensure that ratio of winners to losers is equal in fact_match_stats (as it should be)
    log.info("\nCheck 2: check equal winners and losers in fact_match_stats")

    role_query = f"""
        SELECT player_role, COUNT(*) AS cnt
        FROM `{_full_table_id('fact_match_stats')}`
        GROUP BY player_role
    """

    try:

        # query for tables and number of winners/losers
        role_results = {
            row.player_role: row.cnt
            for row in client.query(role_query).result()
        }

        log.info(f"Roles found: {role_results}")

        # key 'loser' or 'winner' column missing
        if "winner" not in role_results:
            issues.append("fact_match_stats")
        if "loser" not in role_results:
            issues.append("fact_match_stats: no 'loser' rows found")

        # check for roughly equal split (ideally should be 1:1 split)
        if "winner" in role_results and "loser" in role_results:
            w, l = role_results["winner"], role_results["loser"]
            ratio = min(w, l) / max(w, l) if max(w, l) > 0 else 0
            if ratio < 0.90:
                issues.append(
                    f"fact_match_stats: winner/loser split is skewed \
                        ({w:,} vs {l:,}, ratio={ratio:.2f})"
                )
            else:
                log.info(f"Winner/loser split is balanced (ratio={ratio:.2f})")

    except Exception as e:
        issues.append(f"fact_match_stats role check failed: {e}")


    # check 3: tables actually modified
    log.info("\nCheck 3: Table freshness (modified within last 2 hours)")

    freshness_query = f"""
        SELECT 
            table_id,
            TIMESTAMP_MILLIS(last_modified_time) AS last_modified
        FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES__`
        WHERE table_id IN UNNEST(@table_names)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("table_names", "STRING", EXPECTED_TABLES)
        ]
    )

    try:
        # query for tables and their latest modification
        table_modified_times = {
            row.table_id: row.last_modified
            for row in client.query(freshness_query, job_config = job_config).result()
        }

        now = datetime.now(timezone.utc)
        stale_threshold = timedelta(hours = 2)

        for table_name in EXPECTED_TABLES:

            # expected table not found in BigQuery
            if table_name not in table_modified_times:
                issues.append(f"{table_name}: not found in __TABLES__ metadata")
                continue
            
            # calculate age of table
            last_mod = table_modified_times[table_name]
            age = now - last_mod
            if age > stale_threshold:
                msg = f"{table_name}: last modified {age} ago (stale)"
                log.warning(f"WARNING: {msg}")
                issues.append(msg)

            else:
                log.info(f"OK: {table_name}: modified {age} ago")

    except Exception as e:
        log.warning(f"Freshness check failed: {e}")


    # display summary of issues
    if issues:
        summary = "\n - ".join(issues)
        raise AirflowFailException(
            f"Post-load validation failed with {len(issues)} issue(s):\n - {summary}"
        )
    
    log.info("\nPost-load validation passed - all checks green")
