from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.decorators import task

from dotenv import load_dotenv
import requests
import os
import io
import json
import tempfile
import zipfile
import hashlib
from google.cloud import storage

from google.api_core.exceptions import NotFound, Forbidden


# --- CONFIG ---

REPO_URL = f"https://github.com/JeffSackmann/tennis_atp"
BRANCH = "master"
BUCKET_NAME = "tennis-etl-bucket"

def load_env():

    load_dotenv()

    API_TENNIS = os.getenv('API_TENNIS_API_KEY')
    GOOGLE_GCS = os.getenv('GOOGLE_GCS_API_KEY')
    SPORT_DEVS = os.getenv('SPORT_DEVS_API_KEY')


    return API_TENNIS, GOOGLE_GCS, SPORT_DEVS

with DAG(
    dag_id = "github_csv_to_gcs",
    tags = ['bronze', 'github', 'gcs']
) as dag:
    

    

    @task
    def fetch_repo():
        load_env()


        REPO_OWNER = os.getenv("REPO_OWNER")
        REPO_NAME = os.getenv("REPO_NAME")
        BRANCH = os.getenv("BRANCH")


        REPO_URL = f"https://github.com/{REPO_OWNER}/{REPO_NAME}/archive/refs/heads/{BRANCH}.zip"
        response = requests.get(REPO_URL, timeout=120)
        response.raise_for_status()
        response_content = io.BytesIO(response.content)


        # clone via ZIP
        tmp_dir = tempfile.mkdtemp(prefix="gh_")
        zipped_file = zipfile.ZipFile(response_content)
        zipped_file.extractall(tmp_dir)

        

        return