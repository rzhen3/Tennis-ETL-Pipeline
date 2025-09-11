from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from dotenv import load_dotenv
import requests
import os
from pathlib import Path
import io
import json
import tempfile
import zipfile
import hashlib
from google.cloud import storage

from google.api_core.exceptions import NotFound, Forbidden


# --- general variables ---
def load_env():

    load_dotenv()

    API_TENNIS = os.getenv('API_TENNIS_API_KEY')
    GOOGLE_GCS = os.getenv('GOOGLE_GCS_API_KEY')
    SPORT_DEVS = os.getenv('SPORT_DEVS_API_KEY')


    return API_TENNIS, GOOGLE_GCS, SPORT_DEVS


load_env()
REPO_OWNER = os.getenv("REPO_OWNER")
REPO_NAME = os.getenv("REPO_NAME")
BRANCH = os.getenv("BRANCH")
REPO_URL = f"https://github.com/{REPO_OWNER}/{REPO_NAME}/archive/refs/heads/{BRANCH}.zip"
MANIFEST_VAR_NAME = f"manifest_{REPO_OWNER}-{REPO_NAME}"

# --- DAG ---

with DAG(
    dag_id = "github_csv_to_gcs",
    tags = ['bronze', 'github', 'gcs']
) as dag:
    

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
        return csv_indexer
    
    @task
    def save_manifest(manifest, manifest_name = MANIFEST_VAR_NAME):
        """
            Saves manifest as Airflow Variable.
        """
        Variable.set(manifest_name, json.dumps(manifest, indent = 2))


    @task
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
                   if existing_manifest[path_str] != digest
        ]

        merged_manifest = {
            **existing_manifest,
            **{
                path_str: csv_index[path_str] for path_str in changed_csvs
            }
        }

        return {'changed_csvs':changed_csvs, 'merged_manifest':merged_manifest}
    
    @task
    def upload_csvs_to_GCP_bucket(changed_paths, bucket, bucket_prefix, gcp_conn_id):
        """
        Upload only new or updated CSVs into Bronze path.
        """

        if len(changed_paths) == 0:
            return []
        
        hook = GCSHook(gcp_conn_id = gcp_conn_id)


        


        pass

    repo_path = fetch_repo()
    csv_index = hash_csvs(repo_path)

    cmp = compare_with_manifest(csv_index)
    changes_lst = cmp['changed_csvs']
    new_manifest = cmp['merged_manifest']
    save_manifest(new_manifest)



