from dotenv import load_dotenv
import requests
from pathlib import Path
import os
import io
import json
import tempfile
import zipfile
import hashlib

from google.cloud import storage

from google.api_core.exceptions import NotFound, Forbidden


# --- CONFIG ---

def load_env():

    load_dotenv()

    API_TENNIS = os.getenv('API_TENNIS_API_KEY')
    GOOGLE_GCS = os.getenv('GOOGLE_GCS_API_KEY')
    SPORT_DEVS = os.getenv('SPORT_DEVS_API_KEY')


    return API_TENNIS, GOOGLE_GCS, SPORT_DEVS

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
    tmp_dir = tempfile.mkdtemp(prefix="tennis_etl_gh_")
    zipped_file = zipfile.ZipFile(response_content)
    zipped_file.extractall(tmp_dir)
    # root = Path(tmp_dir) / f"{REPO_NAME}-{BRANCH}"
    root = Path(tmp_dir) / f"{REPO_NAME}-{BRANCH}"

    return str(root)

print(fetch_repo())

print(tempfile.gettempdir())