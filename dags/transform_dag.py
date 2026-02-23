import logging

from airflow.sdk import Param
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.datasets import Dataset
from airflow.decorators import task

# from plugins.bq_checks import preflight_schema_check, postload_validation
from bq_checks import preflight_schema_check, postload_validation

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
BRONZE_BASE_NAME = f"bronze/source=github/owner={REPO_OWNER}/repo={REPO_NAME}/ref={BRANCH}"
ARTIFACTS_BASE = "artifacts"

JOBS_URI = f"gs://{BUCKET_NAME}/{ARTIFACTS_BASE}/jobs"
WHEEL_URI = f"gs://{BUCKET_NAME}/{ARTIFACTS_BASE}/wheels/etl_utils-0.1.1-py3-none-any.whl"

BRONZE_DATASET = Dataset(f"gs://{BUCKET_NAME}/{BRONZE_BASE_NAME}/")


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

    # declare param for manual override
    params = {
        "dt": Param(
            default = None,
            type = ["null", "string"],
            pattern = r"^\d{4}-\d{2}-\d{2}$",
            description="Override the processing date partition (YYYY-MM-DD).\n\
                Leave empty for automatic detection from the upload DAG.",
            title="Processing Date Override."
        ),
    },

    # default args
    default_args={
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=5),
        "execution_timeout": dt.timedelta(minutes=45),
    }
) as dag:
    
    @task
    def resolve_processing_date(**context):
        """
        Determine which dt= partition in the GCP bucket to process for remaining tasks.
        
        Prio:
        - Dataset event metadata (from upload DAG)
        - fallback to logical_date (for manual triggers)

        returns date string for downstream tasks to construct GCS path
        """

        # check for manual override via dag_run.conf
        # i.e. airflow dags trigger ... --conf '{"dt": "2026-02-22"}'
        conf = context["dag_run"].conf or {}
        override_dt = conf.get("dt")
        
        if override_dt:
            # validate data format before using
            try:
                dt.datetime.strptime(override_dt, "%Y-%m-%d")
            except ValueError:
                raise ValueError(
                    f"Invalid dt format in dag_run.conf: '{override_dt}'. \n\
                        Expected YYYY-MM-DD."
                )
            
            logging.info(f"Using manual override: dt={override_dt} (from dag_run.conf)")
            return override_dt
        

        # use dataset event metadata.
        # upload_DAG triggers this DAG via dataset outlet.
        events = context.get("triggering_asset_events", {})
        print("DEBUG: LISTING EVENTS-", events.items())

        for dataset_obj, event_list in events.items():

            for event in reversed(event_list):
                extra = event.extra or {}
                dt_value = extra.get("dt")

                # indeed found
                if dt_value:
                    logging.info(
                        f"Resolved dt={dt_value} from dataset event metadata \
                            (prefix={extra.get('prefix', 'N/A')}, \
                                file_count = {extra.get('file_count', 'N/A')})"
                    )
                    return dt_value
                
        # final check, using logical_date
        # last resort using scheduler-assigned timestamp.
        # may not match actual upload date, but better than nothing.
        fallback = context["dag_run"].logical_date.strftime("%Y-%m-%d")
        logging.warning(f"No dataset event metadata found.\n\
                        Falling back to logical_date: dt={fallback}")
        return fallback
    
    processing_date = resolve_processing_date()

    # build input paths using XCom from resolve_processing_date
    _BRONZE_PREFIX = f"gs://{BUCKET_NAME}/{BRONZE_BASE_NAME}/dt="
    _DT = "{{ ti.xcom_pull(task_ids='resolve_processing_date') }}"
    PLAYERS_INPUT = f"{_BRONZE_PREFIX}{_DT}/atp_players.csv"
    MATCHES_INPUT = f"{_BRONZE_PREFIX}{_DT}/atp_matches_*.csv"
    RANKINGS_INPUT = f"{_BRONZE_PREFIX}{_DT}/atp_rankings_*.csv"
    MATCH_STATS_INPUT = MATCHES_INPUT

    _DT_NODASH = (
        "{{ ti.xcom_pull(task_ids='resolve_processing_date') | replace('-', '') }}"
    )

    
    # load players dimension
    # pre-requisite for matches/stats because dim_players is referenced
    # by fact_matches.winner_id / loser_id
    load_players = DataprocCreateBatchOperator(
        task_id="load_players",
        project_id=PROJECT_ID,
        region=REGION,
        gcp_conn_id=GCP_CONN_ID,

        batch_id=f"players-{_DT_NODASH}-{{{{ ti.try_number }}}}",
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
        batch_id="matches-{{ logical_date | ds_nodash }}-{{ ti.try_number }}",
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
        batch_id="rankings-{{ logical_date | ds_nodash }}-{{ ti.try_number }}",
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
        batch_id="match-stats-{{ logical_date | ds_nodash }}-{{ ti.try_number }}",
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
    processing_date >> schema_check >> [load_players, load_matches, load_rankings]

    # match_stats depends on players + matches
    [load_players, load_matches] >> load_match_stats

    # validation run after every job completes
    [load_match_stats, load_rankings] >> validate