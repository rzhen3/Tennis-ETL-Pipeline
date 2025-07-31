import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd


from google.cloud import storage


def load_env():

    load_dotenv()

    API_TENNIS = os.environ['API_TENNIS_API_KEY']

    GOOGLE_GCS = os.environ['GOOGLE_GCS_API_KEY']

    return API_TENNIS, GOOGLE_GCS

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
        
    response = requests.get(
        url = "https://api.api-tennis.com/tennis/?",
        params = {
            "action":"get_standings",
            "APIkey":API_TENNIS_API_KEY,
            "event_type":'ATP'
        },
        verify = False
    )

    json_response = response.json()
    print(json_response)
    print(json_response.keys())

    # upload json to data lake

def upload_to_gcs(data):

    pass

def main():
    load_env()

    send_requests()

