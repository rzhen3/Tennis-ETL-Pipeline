import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from google.cloud import storage


# from google.cloud import storage


def load_env():

    load_dotenv()

    API_TENNIS = os.getenv('API_TENNIS_API_KEY')
    GOOGLE_GCS = os.getenv('GOOGLE_GCS_API_KEY')
    SPORT_DEVS = os.getenv('SPORT_DEVS_API_KEY')

    return API_TENNIS, GOOGLE_GCS, SPORT_DEVS


'''
extract data from endpoint
'''
def send_requests():
    # extract data from endpoint
    # response = requests.get(
    #     url = "https://api.github.com/search/repositories",
    #     params = {"q": "language:python", "sort":"stars", "order":"desc"}
    # )

    # json_response = response.json()
    # repos = json_response['items']
    # for repo in repos[:5]:
    #     print(repo['name'])
    #     print(repo['description'])
    #     print(repo['stargazers_count'])
    SPORT_DEVS_URL = "https://tennis.sportdevs.com/rankings?type=eq.atp&class=eq.official"
    SPORT_DEVS_URL = 'https://tennis.sportdevs.com/'
    SPORT_DEVS = os.getenv('SPORT_DEVS_API_KEY')
    ENDPOINT = 'rankings'
    FULL_URL = f"{SPORT_DEVS}{ENDPOINT}?"
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
        url = SPORT_DEVS_URL,
        data = payload,
        headers = headers,
        verify = False
    )

    json_response = response.json()
    # print(json_response)
    # print(len(json_response))
    print(json_response[0])
    print(json_response[0].keys())

    # store response in a file
    STORAGE_FILE_NAME = f"store_{ENDPOINT}"

    with open(STORAGE_FILE_NAME, 'w') as f:
        json.dump(json_response[0], f)

    


    # upload json to data lake

def upload_to_gcs(bucket_name, blob_name, blob_data):
    # TODO: need to setup 'gcloud auth application-default login' 
    # as an airflow DAG
    # TODO: continue setting up gcloud  

    storage_client = storage.Client()

    buckets = list(storage_client.list_buckets())

    bucket_ref = None
    bucket_exists = False
    for b in buckets:
        if b.name == bucket_name:
            bucket_exists = True
            bucket_ref = b
            break

    if not bucket_exists:
        bucket_ref = storage_client.create_bucket(bucket_name)
        print(f"Bucket {bucket_ref.name} created")

    bucket_ref.blob()
    

def delete_bucket(bucket_name):
    pass

def main():
    print("hello world")
    # load_env()
    send_requests()
    # upload_to_gcs("")

    

main()