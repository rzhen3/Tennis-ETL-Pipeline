import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

from google.cloud import storage

from sqlalchemy import create_engine, text
import psycopg2

from google.api_core.exceptions import NotFound, Forbidden
import traceback


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

    results_dict['payload'] = {
        'limit':30
    }
    results_dict['ENDPOINT'] = "classes"

    results_dict['payload'] = {
        # 'class_id':'eq.416',
        'class_id':'eq.415',
        # 'limit':200
        'name':'like.*Toronto*'
    }
    results_dict['payload'] = {
        # 'limit':200,
        # 'offset':150,
        'class_id':'eq.415'
    }
    results_dict['ENDPOINT'] = 'leagues'

    return results_dict




'''
    extract data from endpoint and return
'''
# TODO: add exception handling
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
    # print(json_response[0]['matches'][0].keys())


    # for i, val in enumerate(json_response[0]['matches']):
    #     print(i, val['name'], \
    #           "t:",val['tournament_name'], \
    #             "l:", val['league_name'], \
    #                 "s:", val['season_name'])
    #     print('---')

    # for i, val in enumerate(json_response[0]['tournaments']):
    #     print(i, val)
    #     # print(i, val['name'])
    #     print('---')

    # for i, val in enumerate(json_response[0]['seasons']):
    #     print(i, val)
    #     print('---')

    # for i, val in enumerate(json_response):
    #     print(i, val)
    #     print('---')

    # store response in a file
    # STORAGE_FILE_NAME = f"store_{ENDPOINT}.json"
    # with open(STORAGE_FILE_NAME, 'w') as f:
    #     json.dump(json_response, f)

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
    [DEPR] upload blob_data to GCS Bucket
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
    


"""
    standard get GCS bucket by name
"""
def get_GCS_bucket(bucket_name):
    gcs_storage = storage.Client()

    bucket_ref = None
    try:
        bucket_ref = gcs_storage.get_bucket(bucket_name)
    except NotFound:
        print('ERROR: bucket not found')
        traceback.print_exc()
        return None
    except Forbidden:
        print("ERROR: bucket access forbidden")
        traceback.print_exc()
        return None
    
    return bucket_ref


"""
    standard upload json-dict to bucket
"""
def upload_json_to_blob(prefix, json_dict, blob_name, bucket_name):
    bucket_ref = get_GCS_bucket(bucket_name)

    blob_data_str = json.dumps(json_dict)
    blob = bucket_ref.blob(f"{prefix}/{blob_name}")

    blob.upload_from_string(blob_data_str, content_type = 'application/json')


def read_data_from_bucket(bucket_name, blob_name):
    
    bucket_ref = get_GCS_bucket(bucket_name)
    if not bucket_ref:
        print("ERROR: bucket not found")
        return None

    # check if blob exists
    blob_ref = bucket_ref.get_blob(blob_name)
    if not blob_ref:
        print("ERROR: blob not found")
        return None
    
    blob_text = blob_ref.download_as_text(encoding = 'utf-8')
    result = json.loads(blob_text)

    print('blob downloaded.')
    return result



"""
    load first 50 leagues into GCS bucket
"""
# TODO: add retry policy to http request sending
# handle possible exceptions to request sending
def load_leagues(offset = 0, limit = 50):

    STD_PIPELINE_BUCKET = "tennis-etl-bucket"
    bucket_ref = get_GCS_bucket(STD_PIPELINE_BUCKET)
    if not bucket_ref:
        print("ERROR: Could not find bucket")
        raise ValueError(f"Could not find bucket {STD_PIPELINE_BUCKET}")


    # send requests to get leagues
    api_key = os.getenv("SPORT_DEVS_API_KEY")
    league_rest_params = {
        "ENDPOINT":"leagues",
        "payload":{
            'class_id':'eq.415', # ATP
            'offset':offset,
            'limit':limit
        },
        "URL":'https://tennis.sportdevs.com/',
        'headers':{
            'Accept':'application/json',
            'Authorization': "Bearer "+api_key
        }
    }
    leagues_data = send_requests(league_rest_params)
    print("sent and retrieved leagues http request.")

    # iterate through the names of the leagues and retrieve them 
    folder_prefix = "leagues"
    for league in leagues_data:

        league_id = league['id']
        league_name = league['name']

        # make league name suitable
        league_name = league_name.strip()
        league_name = league_name.replace(',', '_')
        league_name = league_name.replace(' ', '_')
        league_prefix = f"league_{league_name}"
        
        # create blob for general information
        league_data_str = json.dumps(league)

        GENERAL_BLOB_NAME = f"{folder_prefix}/{league_prefix}/general_info"
        if bucket_ref.get_blob(GENERAL_BLOB_NAME) is None:
            general_blob = bucket_ref.blob(GENERAL_BLOB_NAME)
            general_blob.upload_from_string(league_data_str, 
                                            content_type = 'application/json')
        
        # retrieve seasons for the leagues
        season_rest_params = {
            "ENDPOINT":"seasons",
            "payload":{
                'league_id':f"eq.{league_id}"
            },
            "URL":'https://tennis.sportdevs.com/',
            'headers':{
                'Accept':'application/json',
                'Authorization': "Bearer "+api_key
            }
        }
        seasons_data = send_requests(season_rest_params)


        # upload retrieved seasons under the leg
        for season in seasons_data:
            season_data_str = json.dumps(season)
            BLOB_NAME = f"{folder_prefix}/{league_prefix}/{season['year']}_season"

            if bucket_ref.get_blob(BLOB_NAME) is None:
                season_blob = bucket_ref.blob(BLOB_NAME)
                season_blob.upload_from_string(
                    season_data_str,
                    content_type = "application/json"
                )

"""
    load player data from a certain year
"""
# TODO: still have to figure out the most efficient way to get all seasons of 
def load_players(year = 2025):

    NUM_LEAGUES_TO_CHECK = 50
    STD_PIPELINE_BUCKET = 'tennis-etl-bucket'

    bucket_ref = get_GCS_bucket(STD_PIPELINE_BUCKET)
    if not bucket_ref:
        print("ERROR: bucket could not be found")
        raise ValueError("Bucket could not be found")
    
    leagues_checked = 0
    limit = 50
    offset = 0
    valid_leagues = []
    while leagues_checked < NUM_LEAGUES_TO_CHECK:
        api_key = os.getenv("SPORT_DEVS_API_KEY")
        leagues_rest_params = {
            'ENDPOINT':'leagues',
            'URL':'https://tennis.sportdevs.com/',
            'headers':{
                'Accept':'application/json',
                'Authorization': 'Bearer '+api_key
            },
            'payload':{
                'class_id':'eq.415', # ATP
                'limit':limit,
                offset:offset
            }
        }
        leagues_data = send_requests(leagues_rest_params)
        print("sent and retrieved leagues data")

        for league in leagues_data:
            league_date = league['']


        leagues_checked += limit


    


    # keep league if last played within the given year

     
    


def sending_test2():
    api_key = os.getenv("SPORT_DEVS_API_KEY")
    teams_rest_params = {
        "ENDPOINT":"teams",
        "payload":{
            # 'category_id':'eq.415', # ATP
        },
        "URL":'https://tennis.sportdevs.com/',
        'headers':{
            'Accept':'application/json',
            'Authorization': "Bearer "+api_key
        }
    }
    player_data = send_requests(teams_rest_params)

    print('...')
    for i in player_data:
        print(i)
        print('---')


def sending_test():
    league_id = 1942
    api_key = os.getenv("SPORT_DEVS_API_KEY")
    league_rest_params = {
        "ENDPOINT":"leagues",
        "payload":{
            'class_id':'eq.415', # ATP
            # 'offset':offset,
            # 'limit':limit
            # 'name':'like.*Wimbledon*'
        },
        "URL":'https://tennis.sportdevs.com/',
        'headers':{
            'Accept':'application/json',
            'Authorization': "Bearer "+api_key
        }
    }
    league_data = send_requests(league_rest_params)
    print(league_data)

    print('...')
    for i in league_data:
        print(i)
        print('---')

def sending_test3():
    league_id = 1942
    api_key = os.getenv("SPORT_DEVS_API_KEY")
    league_rest_params = {
        "ENDPOINT":"leagues-by-date",
        "payload":{
            'date':'eq.2025-08-13',
            # 'class_id':'eq.415',
            # 'offset':offset,
            # 'limit':limit
            # 'name':'like.*Wimbledon*'
        },
        "URL":'https://tennis.sportdevs.com/',
        'headers':{
            'Accept':'application/json',
            'Authorization': "Bearer "+api_key
        }
    }
    league_data = send_requests(league_rest_params)
    print(league_data)

    print('...')
    for i in league_data[0]['leagues']:
        print(i)
        print('---')




"""
    retrieve leagues from GCS bucket
"""
def retrieve_leagues():

    # retrieve GCS league data
    GCS_BUCKET_NAME = 'tennis-etl-bucket'
    GCS_BLOB_NAME = 'leagues'
    data = read_data_from_bucket(GCS_BUCKET_NAME, GCS_BLOB_NAME)
    if not data:
        return
    print("retrieved gcs data as:")
    for i, val in enumerate(data):
        print(i, val)
        print('---')

    






"""
    retrieve seasons for leagues
"""
def retrieve_seasons():
    pass

def main():
    print("starting...")
    load_env()

    sending_test()
    # load_leagues()
    # retrieve_leagues()
    


    # REST_param_dict = get_rest_params()

    # ranking_data = send_requests(REST_param_dict)


    # TODO: load first 50 leagues, and upload to GCS bucket. 
    # load first 50 leagues


    # upload_to_gcs("tennis-etl-bucket", "atp_rankings", ranking_data)
    # upload_to_gcs("test-bucket-bxkjxzk", "hello", "its_me")

    # output = read_data_from_bucket("tennis-etl-bucket", "atp_rankings")
    # print(type(output))


    # engine = setup_postgres_db()

    # TODO: upload ranking data in a way to make retrieving data from bucket easy
    # b/c currently, data retrieved is a stringified array

    # TODO: add exception handling to upload code

    # TODO: setup GCS buckets with Terraform
    

main()