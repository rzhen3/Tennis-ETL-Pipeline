
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
    "fact_rankings":     1000,
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
                Run: python scripts/manage_bigquery.py create"
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
    log.info("Check 1: Min row count thresholds met")

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
        except Exception as e:
            msg = f"{table_name}: error reading metadata"
            log.error(f"ERROR: {msg}")
            issues.append(msg)
    
    
    # check 2: checking for null foreign keys
    log.info("\nCheck 2: NULL player_id FKs in fact_matches")
    null_fk_query = f"""
        SELECT COUNTIF(winner_id IS NULL) AS null_winners,
            COUNTIF(loser_id IS NULL) AS null_losers,
            COUNT(*) AS total_rows 
        FROM `{_full_table_id('fact_matches')}`
    """ 
    try:
        row = list(client.query(null_fk_query).result())[0]
        if row.null_winners > 0 or row.null_losers > 0:
            issues.append(
                f"fact_matches: NULL foreign keys detected - \
                    # winner_id nulls={row.null_winners}, # loser_id nulls={row.null_losers} \
                    (out of {row.total_rows:,} rows)"
            )
        else:
            log.info("OK: fact_matches has no NULL foreign keys")
    except Exception as e:
        issues.append(f"fact_matches NULL FK check errored - {e}")


    # check 3: ensure that ratio of winners to losers is equal in fact_match_stats (as it should be)
    log.info("\nCheck 3: check equal winners and losers in fact_match_stats")

    role_query = f"""
        SELECT 
            COUNTIF(player_role = 'winner') AS num_winners,
            COUNTIF(player_role = 'loser') AS num_losers
        FROM `{_full_table_id('fact_match_stats')}`

    """

    try:
        # query for tables and number of winners/losers
        row = list(client.query(role_query).result())[0]
        
        if row.num_winners == 0 or row.num_losers == 0:
            issues.append(f"fact_match_stats: missing player roles - \
                          # winners={row.num_winners}, # losers={row.num_losers}")
        else:
            ratio = min(row.num_winners, row.num_losers) / max(row.num_winners, row.num_losers)
            if ratio >= 0.999:
                log.info(f"OK: fact_match_stats winner/loser split is perfectly balanced (ratio=1.00)")
            elif ratio < 0.95:
                issues.append(f"fact_match_stats: role imbalance - \
                              # winners={row.num_winners}, # losers={row.num_losers}, (ratio={ratio:.3f})")
            else:
                log.info(f"OK: fact_match_stats winner/loser split is roughly balanced (ratio={ratio:.3f})")

    except Exception as e:
        issues.append(f"fact_match_stats role check failed: {e}")

    # check 4: check for orphaned FKs in matches
    log.info("\nCheck 4: orphaned player_id FKs in matches")
    fk_orphans = f"""
        SELECT 
            COUNTIF(p1.player_id IS NULL) AS orphaned_winners,
            COUNTIF(p2.player_id IS NULL) AS orphaned_losers,
            COUNT(*) AS total_rows
        FROM `{_full_table_id('fact_matches')}` m
        LEFT JOIN `{_full_table_id('dim_players')}` p1 ON m.winner_id = p1.player_id 
        LEFT JOIN `{_full_table_id('dim_players')}` p2 ON m.loser_id = p2.player_id
    """
    try:
        row = list(client.query(fk_orphans).result())[0]
        if row.orphaned_winners > 0 or row.orphaned_losers > 0:
            issues.append(f"fact_matches: orphaned foreign keys detected - \
                          winner_id orphans={row.orphaned_winners}, loser_id orphans={row.orphaned_losers}, (out of {row.total_rows:,})")
        else:
            log.info("OK: fact_matches has no FK orphans with dim_players")
    except Exception as e:
        issues.append(f"fact_matches orphaned FKs errored - {e}")


    # check 5: tables actually modified
    log.info("\nCheck 5: Table freshness (modified within last 2 hours)")

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
        issues.append(f"Freshness check failed: {e}")
        log.warning(f"Freshness check failed: {e}")


    # display summary of issues
    if issues:
        summary = "\n - ".join(issues)
        raise AirflowFailException(
            f"Post-load validation failed with {len(issues)} issue(s):\n - {summary}"
        )
    
    log.info("\nPost-load validation passed - all checks green")
