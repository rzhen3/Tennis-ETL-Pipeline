import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from google.cloud import storage
from sqlalchemy import create_engine, text
import psycopg2

# from google.cloud import storage


def load_env():

    load_dotenv()

    API_TENNIS = os.getenv('API_TENNIS_API_KEY')
    GOOGLE_GCS = os.getenv('GOOGLE_GCS_API_KEY')
    SPORT_DEVS = os.getenv('SPORT_DEVS_API_KEY')


    return API_TENNIS, GOOGLE_GCS, SPORT_DEVS


'''
    extract data from endpoint and return
'''
def send_requests():
    # extract data from endpoint

    # SPORT_DEVS_URL = "https://tennis.sportdevs.com/rankings?type=eq.atp&class=eq.official"
    SPORT_DEVS_URL = 'https://tennis.sportdevs.com/'
    SPORT_DEVS = os.getenv('SPORT_DEVS_API_KEY')
    
    ENDPOINT = 'rankings'
    FULL_URL = f"{SPORT_DEVS_URL}{ENDPOINT}?"
    payload = {
        'type': 'atp',
        'class': 'official'
    }
    
    headers = {
        'Accept':'application/json',
        'Authorization': "Bearer " +SPORT_DEVS
    }
    
    print('------- sending requests --------')
        
    response = requests.get(
        url = FULL_URL,
        data = payload,
        headers = headers,
        verify = False
    )

    json_response = response.json()
    print(json_response)
    print(len(json_response))
    print(json_response[0])
    print(json_response[0].keys())

    # store response in a file
    STORAGE_FILE_NAME = f"store_{ENDPOINT}.json"

    with open(STORAGE_FILE_NAME, 'w') as f:
        json.dump(json_response, f)
        # json.dump(json_response[0], f)

    return json_response

    

def get_postgres_creds():
    load_dotenv()
    PG_USERNAME = os.getenv('POSTGRES_USERNAME')
    PG_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    HOST = os.getenv('DB_HOST')
    PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    return PG_USERNAME, PG_PASSWORD, HOST, PORT, DB_NAME


def setup_postgres_db():
    # TODO: consider setting up an airflow dag for creating local pgdb
    DB_BACKEND = "postgres"
    DB_API = "psycopg2"

    USERNAME, PASSWORD, HOST, PORT, DB_NAME = get_postgres_creds()
    connection_string = f"{DB_BACKEND}+{DB_API}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"

    engine = create_engine(connection_string, echo = True)
    return engine

"""
    upload blob_data to GCS Bucket
"""
def upload_to_gcs(bucket_name, blob_name, blob_data):
    # TODO: need to setup 'gcloud auth application-default login' 
    # as an airflow DAG
    # TODO: continue setting up gcloud  

    storage_client = storage.Client()

    buckets = list(storage_client.list_buckets())

    bucket_ref = None
    bucket_exists = False
    for b in buckets:
        print(b.name)
        if b.name == bucket_name:
            bucket_exists = True
            bucket_ref = b
            break

    if not bucket_exists:
        bucket_ref = storage_client.create_bucket(bucket_name)
        print(f"Bucket {bucket_ref.name} created")

    blob_to_upload = bucket_ref.blob(blob_name)
    blob_to_upload.upload_from_file()
    blob_to_upload.upload_from_file(blob_data)
    

def delete_bucket(bucket_name):
    pass

def main():
    print("hello world")
    load_env()
    ranking_data = send_requests()
    upload_to_gcs("test-bucket-bxkjxzk", "hello", "its_me")

    # engine = setup_postgres_db()

    # TODO: a pull rankings data and upload to gcs bucket
    # then pull rankings data from gcs bucket and output here
    

main()