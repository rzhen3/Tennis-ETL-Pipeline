from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.datasets import Dataset

from plugins.bq_checks import preflight_schema_check, postload_validation

from dotenv import load_dotenv
import os
import datetime as dt
import re

# loading .env variables
load_dotenv(f"{os.getcwd()}/secrets/.env")
BUCKET_NAME = os.getenv("BUCKET_NAME")
REPO_OWNER = os.getenv("REPO_OWNER")
REPO_NAME = os.getenv("REPO_NAME")
BRANCH = os.getenv("BRANCH")

# GCP settings
PROJECT_ID = "tennis-etl-pipeline"
REGION = "us-central1"
GCP_CONN_ID = "google_cloud_default"
DATAPROC_SERVICE_ACCOUNT = os.getenv(
    "DATAPROC_SERVICE_ACCOUNT",
    "dataproc-tennis-etl@tennis-etl-pipeline.iam.gserviceaccount.com"
)

# GCS paths
BRONZE_BASE = f"bronze/source=github/owner={REPO_OWNER}/repo={REPO_OWNER}/ref={BRANCH}"
ARTIFACTS_BASE = "artifacts"

JOBS_URI = f"gs://{BUCKET_NAME}/{ARTIFACTS_BASE}/jobs"
WHEEL_URI = f"gs://{BUCKET_NAME}/{ARTIFACTS_BASE}/wheels/etl_utils-0.1.1-py3-none-any.whl"

BRONZE_DATASET = Dataset(f"gs://{BUCKET_NAME}/{BRONZE_BASE}")


# build dataproc serverless batch config
def make_batch_config(job_filename: str, args: list[str]) -> dict:
    """
    build batch configuration dict for DataprocCreateBatchOperator.
    
    """
    return {
        "pyspark_batch": {
            "main_python_file_uri": f"{JOBS_URI}/{job_filename}",
            "python_file_uris": [WHEEL_URI],
            "args": args,
        },
        "runtime_config": {
            "version": "2.2",
        },
        "environment_config": {
            "execution_config": {
                "service_account": DATAPROC_SERVICE_ACCOUNT,
                "subnetwork_uri": f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default",
            }
        }
    }

def sanitize_batch_id(raw_id: str) -> str:
    """
    sanitize string into a valid Dataproc batch ID.
    converts invalid character to hyphen and enforces the constraints.
    enforces:
        - 4 to 63 chars long
        - only lowercase letters, numbers, and hyphens
        - start with lowercase letter
        - no ending with hyphen

    """
    sanitized = re.sub(r"[^a-z0-9\-]", "-", raw_id.lower())
    sanitized = re.sub(r"-+", "-", sanitized)
    sanitized = sanitized.strip("-")

    if sanitized and not sanitized[0].isalpha():
        sanitized = "b-" + sanitized

    sanitized = sanitized[:63].rstrip()
    return sanitized

# input paths for jobs. auto-fill with logical date
_BRONZE_PREFIX = f"gs://{BUCKET_NAME}/{BRONZE_BASE}/dt="
PLAYERS_INPUT = _BRONZE_PREFIX + "{{ ds }}/atp_players.csv"
MATCHES_INPUT = _BRONZE_PREFIX + "{{ ds }}/atp_matches_*.csv"
RANKINGS_INPUT = _BRONZE_PREFIX + "{{ ds }}/atp_rankings_*.csv"

MATCH_STATS_INPUT = MATCHES_INPUT

# DAG definition
with DAG(
    dag_id = "transform_bronze_to_warehouse",
    tags=["silver", "transform", "dataproc", "bigquery"],
    description="run PySpark transform jobs on dataproc serverless to\
        load bronze CSV data into BigQuery warehouse tables",

    # dataset trigger for DAG run
    # from 'outlets=[BRONZE_DATASET]'
    schedule=[BRONZE_DATASET],

    start_date = dt.datetime(2025, 2, 1),
    catchup = False,

    # default args
    default_args={
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=5),
        "execution_timeout": dt.timedelta(minutes=45),
    }
) as dag:
    
    # load players dimension
    # pre-requisite for matches/stats because dim_players is referenced
    # by fact_matches.winner_id / loser_id
    load_players = DataprocCreateBatchOperator(
        task_id="load_players",
        project_id=PROJECT_ID,
        region=REGION,
        gcp_conn_id=GCP_CONN_ID,

        batch_id="players-{{ ds_nodash }}-{{ ti.try_number }}",
        batch=make_batch_config(
            job_filename="load_players.py",
            args=["--input_path", PLAYERS_INPUT],
        )
    )


    # load matches + derive tournaments
    # produces fact_matches and dim_tournaments
    # prerequisite for load_match_stats
    load_matches = DataprocCreateBatchOperator(
        task_id="load_matches",
        project_id=PROJECT_ID,
        region=REGION,
        gcp_conn_id=GCP_CONN_ID,
        batch_id="matches-{{ ds_nodash }}-{{ ti.try_number }}",
        batch=make_batch_config(
            job_filename="load_matches.py",
            args=["--input_path", MATCHES_INPUT]
        )
    )


    # load rankings
    # independent of other tasks
    load_rankings = DataprocCreateBatchOperator(
        task_id="load_rankings",
        project_id = PROJECT_ID,
        region=REGION,
        gcp_conn_id=GCP_CONN_ID,
        batch_id="rankings-{{ ds_nodash }}-{{ ti.try_number }}",
        batch=make_batch_config(
            job_filename="load_rankings.py",
            args=["--input_path", RANKINGS_INPUT]
        )
    )


    # load match stats
    # needs player_id (from dim_players)
    # needs tourney_id (from dim_tournaments, created by load_matches)
    load_match_stats = DataprocCreateBatchOperator(
        task_id="load_matches_stats",
        project_id=PROJECT_ID,
        region=REGION,
        gcp_conn_id=GCP_CONN_ID,
        batch_id="match-stats-{{ ds_nodash }}-{{ ti.try_number }}",
        batch=make_batch_config(
            job_filename="load_matches_stats.py",
            args=["--input_path", MATCH_STATS_INPUT]
        ),
    )

    # pre-flight: verify BQ tables exist before Dataproc
    schema_check = PythonOperator(
        task_id="preflight_schema_check",
        python_callable=preflight_schema_check,
        retries = 0
    )

    # post-load: validate data after proc jobs complete
    validate = PythonOperator(
        task_id = "postload_validation",
        python_callable = postload_validation,
        retries = 1,
        retry_delay=dt.timedelta(minutes=2),
    )

    # run pre-requisites

    # schema must exist before job runs
    schema_check >> [load_players, load_matches, load_rankings]

    # match_stats depends on players + matches
    [load_players, load_matches] >> load_match_stats

    # validation run after every job completes
    [load_match_stats, load_rankings] >> validate