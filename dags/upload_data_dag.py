from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.decorators import task
# from airflow.models import Variable
from airflow.sdk import Metadata, Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateBatchOperator
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException
# from airflow.utils.context import get_current_context

import logging

from dotenv import load_dotenv
import requests
import os
from pathlib import Path
import datetime as dt
import io
import json
import tempfile
import zipfile
import hashlib
from google.cloud import storage


from google.api_core.exceptions import NotFound, Forbidden

# TODO: error/exception handling
# TODO: scheduled checks for updates in github

# --- general variables ---
def load_env():

    logging.info(f"my current path is:{os.getcwd()}")

    load_dotenv(f"{os.getcwd()}/secrets/.env")

    API_TENNIS = os.getenv('API_TENNIS_API_KEY')
    GOOGLE_GCS = os.getenv('GOOGLE_GCS_API_KEY')
    SPORT_DEVS = os.getenv('SPORT_DEVS_API_KEY')


    return API_TENNIS, GOOGLE_GCS, SPORT_DEVS


load_env()
REPO_OWNER = os.getenv("REPO_OWNER")
REPO_NAME = os.getenv("REPO_NAME")
BRANCH = os.getenv("BRANCH")
BUCKET_NAME = os.getenv("BUCKET_NAME")
PROJECT_ID = os.getenv("PROJECT_ID")
REPO_URL = f"https://github.com/{REPO_OWNER}/{REPO_NAME}/archive/refs/heads/{BRANCH}.zip"
MANIFEST_VAR_NAME = f"AIRFLOW_VAR_MANIFEST_{REPO_OWNER}_{REPO_NAME}"
GCP_CONN_ID = "google_cloud_default"
BRONZE_BASE_NAME = f"bronze/source=github/owner={REPO_OWNER}/repo={REPO_NAME}/ref={BRANCH}"
# PROJECT_ID = "tennis-etl-pipeline"

ARTIFACT_BASE_NAME = f"artifacts"

BRONZE_DATASET = Dataset(f"gs://{BUCKET_NAME}/{BRONZE_BASE_NAME}/")
ARTIFACT_DATASET = Dataset(f"gs://...")     # need to add

# --- DAG ---

with DAG(
    dag_id = "upload_data_pipeline",
    tags = ['bronze', 'github', 'gcs'],
    schedule = "0 2 * * 0",
    start_date = dt.datetime(2025, 2, 1),
    catchup = False
) as dag:
    @task
    def ensure_bucket(bucket_name=BUCKET_NAME, project_name=PROJECT_ID, gcp_conn_id = GCP_CONN_ID):


        hook = GCSHook(gcp_conn_id = gcp_conn_id)

        if hook.exists(bucket_name = bucket_name):
            return
        
        hook.create_bucket(
            bucket_name = bucket_name,
            storage_class = "STANDARD",
            location = "US",
            project_id = project_name
        )
        logging.info(f"created bucket '{bucket_name}'.")

    @task
    def fetch_repo():
        """
            Download Github branch archive as tempfile then return root
        """
        response = requests.get(REPO_URL, timeout=120)
        response.raise_for_status()
        response_content = io.BytesIO(response.content)


        # clone via ZIP
        tmp_dir = tempfile.mkdtemp(prefix="tennis_etl_gh_")
        zipped_file = zipfile.ZipFile(response_content)
        zipped_file.extractall(tmp_dir)
        root = Path(tmp_dir) / f"{REPO_NAME}-{BRANCH}"

        logging.info(f"Extracted repo to: {root}")
        return str(root)
    
    @task
    def hash_csvs(repo_path):
        """
            build snapshop of all CSVs
        """
        base = Path(repo_path)
        csv_indexer = {}

        path_glob = base.glob('**/*.csv')

        file_checks = [
            "atp_players.csv",
            "atp_rankings_[0-9][0-9]s.csv",
            "atp_rankings_current.csv",
            "atp_matches_[0-9][0-9][0-9][0-9].csv"
        ]

        # scan all files
        for p in path_glob:

            # only add specific CSVs to manifest
            valid = False
            for check_str in file_checks:
                if p.match(check_str):
                    valid = True

            if not p.is_file() or not valid:
                continue

            # feed hash MB by MB
            hasher = hashlib.sha256()
            with p.open("rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    hasher.update(chunk)

            rel = str(p.relative_to(base))
            csv_indexer[rel] = hasher.hexdigest()
        
        # store indexer locally
        logging.info(f"Hashed {len(csv_index)} CSV file(s)")
        return csv_indexer
    
    @task
    def save_manifest(manifest, manifest_name = MANIFEST_VAR_NAME):
        """
            Saves manifest as Airflow Variable.
        """
        Variable.set(manifest_name, json.dumps(manifest, indent = 2))
        logging.info(f"Manifest saved to Airflow Variable '{manifest_name}'.")


    @task(multiple_outputs = True)
    def compare_with_manifest(csv_index, manifest_var = MANIFEST_VAR_NAME):
        """
            Compare current CSV indexer with last manifest.
            Update only changed files.
        """

        try:
            existing_manifest = json.loads(
                Variable.get(manifest_var)
            )
        except Exception:
            existing_manifest = {}

        changed_csvs = [
            path_str for path_str, digest in csv_index.items() 
                   if (not path_str in existing_manifest) or existing_manifest[path_str] != digest
        ]

        logging.info(f"Changed/new CSVs: {changed_csvs}")
        merged_manifest = {
            **existing_manifest,
            **{
                path_str: csv_index[path_str] for path_str in changed_csvs
            }
        }

        return {'changed_csvs':changed_csvs, 'merged_manifest':merged_manifest}
    
    @task(outlets=[BRONZE_DATASET])
    def upload_csvs_to_gcs(changed_paths, repo_path, 
                                  bucket_prefix = BRONZE_BASE_NAME, 
                                  bucket_name = BUCKET_NAME,
                                gcp_conn_id = GCP_CONN_ID,
                                outlet_events = None):
        """
        Upload only new or updated CSVs into Bronze path.
        Attach path metadata as Dataset event so that downstream tasks can read the correct data.
        """

        if len(changed_paths) == 0:
            raise AirflowSkipException("No changed CSVs - skipping upload and dataset event.")
        
        hook = GCSHook(gcp_conn_id = gcp_conn_id)
        date_str = dt.date.today().isoformat()
        dest_prefix = f"{bucket_prefix}/dt={date_str}"

        uploaded_csvs = []

        for rel in changed_paths:
            src = Path(repo_path) / rel
            blob_name = f"{dest_prefix}/{src.name}"

            # upload blob via hook
            hook.upload(
                bucket_name = bucket_name,
                object_name = blob_name,
                filename = str(src),
                mime_type = "text/csv",
            )

            uploaded_csvs.append(f"gs://{bucket_name}/{blob_name}")
            logging.info(f"Uploaded: {blob_name}")

        # attach upload date and more as metadata to event
        outlet_events[BRONZE_DATASET].extra = {
            "dt": date_str,
            "prefix": f"gs://{bucket_name}/{blob_name}",
            "file_count": len(uploaded_csvs)
        }

        logging.info(f"Uploaded {len(uploaded_csvs)} CSV file(s) to Bronze layer (dt={date_str}).")
        return uploaded_csvs
    
    @task
    def upload_jobs_to_gcs(bucket_name = BUCKET_NAME, bucket_prefix = ARTIFACT_BASE_NAME, gcp_conn_id = GCP_CONN_ID):
        hook = GCSHook(gcp_conn_id = gcp_conn_id)

        mime_map = {
            ".py": "text/x-python",
            ".whl": "application/zip",
            ".json": "application/json",
            ".yaml": "application/yaml",
            ".yml": "application/yaml",
        }

        uploads = []
        artifact_dir = Path("./artifacts")
        subdirs = ["jobs", "wheels", "configs"]
        
        for subdir in subdirs:

            full_path = artifact_dir / subdir

            if not full_path.exists():
                logging.warning(f"Artifact subdirectory not found, skipping: {full_path}")
                continue
            for file in full_path.iterdir():

                if not file.is_file():
                    continue

                if file.is_file():
                    full_bucket_path = f"{bucket_prefix}/{subdir}"

                    file_path = Path(file.absolute())
                    blob_name = f"{full_bucket_path}/{file.name}"
                    mime_type = mime_map.get(file.suffix.lower(), "application/octet-stream")

                    hook.upload(
                        bucket_name = bucket_name,
                        object_name = blob_name,
                        filename = str(file_path),
                        mime_type = mime_type
                    )

                    uploads.append(f"gs://{bucket_name}/{blob_name}")
                    logging.info(f"Uploaded artifact: {blob_name}")

        logging.info(f"Uploaded {len(uploads)} artifact file(s) to GCS")
        return uploads


    bucket_ready        = ensure_bucket(BUCKET_NAME, PROJECT_ID)
    local_repo          = fetch_repo()
    csv_index           = hash_csvs(local_repo)
    comparison          = compare_with_manifest(csv_index)
    changed_list        = comparison["changed_csvs"]
    merged_manifest     = comparison["merged_manifest"]
    csvs_uploaded       = upload_csvs_to_gcs(changed_list, local_repo)
    jobs_uploaded       = upload_jobs_to_gcs()
    manifest_saved      = save_manifest(merged_manifest)

    # explicitly order tasks with no XCom connection
    bucket_ready >> [local_repo, jobs_uploaded]