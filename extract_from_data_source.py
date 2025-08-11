import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from google.cloud import storage
from sqlalchemy import create_engine, text
import psycopg2


"""
    get basic credentials
"""
def load_env():

    load_dotenv()

    API_TENNIS = os.getenv('API_TENNIS_API_KEY')
    GOOGLE_GCS = os.getenv('GOOGLE_GCS_API_KEY')
    SPORT_DEVS = os.getenv('SPORT_DEVS_API_KEY')


    return API_TENNIS, GOOGLE_GCS, SPORT_DEVS



"""
    configure REST request parameters
"""
def get_rest_params():

    load_env()

    api_key = os.getenv("SPORT_DEVS_API_KEY")

    results_dict = dict()
    results_dict['URL'] = 'https://tennis.sportdevs.com/' 
    # FIRST PAYLOAD
    # results_dict['ENDPOINT'] = 'rankings'
    # results_dict['payload'] = {
    #     'type':'atp',
    #     'class':'now'
    # }

    # SECOND PAYLOAD
    # results_dict['payload'] = {
    #     'limit':35
    # }
    # results_dict['ENDPOINT'] = 'leagues'
    results_dict['headers'] = {
        'Accept':'application/json',
        'Authorization': "Bearer " +api_key
    }



    # THIRD PAYLOAD
    # results_dict['payload'] = {
    #     'limit':35,
        
    #     # "leagueid":"2288"
    # }
    # results_dict['ENDPOINT'] = 'tournaments'

    # FOURTH PAYLOAD
    results_dict['payload'] = {
        # 'league_id':'eq.2288',
        # 'league_id':'eq.2601',
        'league_id':'eq.2524',

    }
    results_dict['ENDPOINT'] = 'tournaments-by-league'



    # FIFTH PAYLOAD
    # results_dict['payload'] = {
    #     'league_id':'eq.2524'
    # }
    # results_dict['ENDPOINT'] = 'seasons'

    # results_dict['payload'] = {
    #     'league_id': 'eq.2524'
    # }
    # results_dict['ENDPOINT'] = 'seasons-by-league'
    

    # ANOTHER PAYLOAD

    # results_dict['payload'] = {
    #     'league_id': 'eq.2524'
    # }
    # results_dict['ENDPOINT'] = 'leagues-info'


    # results_dict['payload'] = {
    #     # 'tournament_id':'eq.24091',
    #     'tournament_id':'eq.24223'

    # }
    # results_dict['ENDPOINT'] = 'seasons-by-tournament'

    results_dict['payload'] = {
        'date': 'eq.2025-08-10',
    }
    results_dict['ENDPOINT'] = "matches-by-date"

    return results_dict




'''
    extract data from endpoint and return
'''
def send_requests(REST_params):
    # extract data from endpoint

    URL = REST_params['URL']
    ENDPOINT = REST_params['ENDPOINT']
    
    FULL_URL = f"{URL}{ENDPOINT}?"
    payload = REST_params['payload']
    headers = REST_params['headers']
    
    
    print('------- sending requests --------')
        
    response = requests.get(
        url = FULL_URL,
        params = payload,
        headers = headers,
        verify = False
    )

    json_response = response.json()
    # print(json_response)
    # print(len(json_response))
    # print(json_response[0])
    # print(json_response[0].keys())
    # print(json_response[0]['tournaments'])
    print(json_response[0]['matches'][0].keys())

    for i, val in enumerate(json_response[0]['matches']):
        print(i, val['name'], \
              "t:",val['tournament_name'], \
                "l:", val['league_name'], \
                    "s:", val['season_name'])
        print('---')

    # for i, val in enumerate(json_response[0]['tournaments']):
    #     print(i, val)
    #     # print(i, val['name'])
    #     print('---')

    # for i, val in enumerate(json_response[0]['seasons']):
    #     print(i, val)
    #     print('---')

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

    # scan visible buckets
    bucket_ref = None
    bucket_exists = False
    for b in buckets:
        print(b.name)
        if b.name == bucket_name:
            bucket_exists = True
            bucket_ref = b
            break

    # create new bucket if it does not exist
    if not bucket_exists:
        bucket_ref = storage_client.create_bucket(bucket_name)
        print(f"Bucket {bucket_ref.name} created")

    blob_to_upload = bucket_ref.blob(blob_name)

    blob_data_as_str = json.dumps(blob_data)

    blob_to_upload.upload_from_string(blob_data_as_str, content_type = 'application/json')
    print("uploaded data to GCS bucket...")
    # blob_to_upload.upload_from_file(blob_data)
    

def delete_bucket(bucket_name):
    pass

def read_data_from_bucket(bucket_name, blob_name):
    result = None

    storage_client = storage.Client()
    buckets = list(storage_client.list_buckets())

    # check if bucket exists
    bucket_exists = False
    bucket_ref = None
    for b in buckets:
        if b.name == bucket_name:
            bucket_exists = True
            bucket_ref = b

    # exit early if bucket does not exist
    if not bucket_exists:
        print(f"bucket {bucket_name} does not exist")
        return result
    print("bucket exists...")
    
    # retrieve blob from bucket
    blob_ref = None
    blob_exists = False
    for blob in bucket_ref.list_blobs():
        if blob.name == blob_name:
            blob_ref = blob
            blob_exists = True

    if not blob_exists:
        print(f"blob {blob_name} does not exist")
        return result
    print("blob exists...")
    
    print("found both bucket and blob")
    result = blob_ref.download_as_text()
    print(result)

    return result

def main():
    print("hello world")
    load_env()

    REST_param_dict = get_rest_params()

    ranking_data = send_requests(REST_param_dict)
    # upload_to_gcs("tennis-etl-bucket", "atp_rankings", ranking_data)
    # upload_to_gcs("test-bucket-bxkjxzk", "hello", "its_me")

    # output = read_data_from_bucket("tennis-etl-bucket", "atp_rankings")
    # print(type(output))


    # engine = setup_postgres_db()

    # TODO: upload ranking data in a way to make retrieving data from bucket easy
    # b/c currently, data retrieved is a stringified array

    # TODO: add exception handling to upload code
    

main()