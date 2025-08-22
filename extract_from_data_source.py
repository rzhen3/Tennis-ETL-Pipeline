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

import response_cache


STD_PIPELINE_BUCKET = 'tennis-etl-bucket'


"""
    get basic credentials
"""
def load_env():

    load_dotenv()

    API_TENNIS = os.getenv('API_TENNIS_API_KEY')
    GOOGLE_GCS = os.getenv('GOOGLE_GCS_API_KEY')
    SPORT_DEVS = os.getenv('SPORT_DEVS_API_KEY')


    return API_TENNIS, GOOGLE_GCS, SPORT_DEVS


'''
    return cached response if has been cached.
    else, extract data from endpoint and return
'''
# TODO: add exception handling
def send_requests(REST_params, with_cache = True):

    # check if response has been cached
    if with_cache:
        cached_response = response_cache.get(REST_params)
        if cached_response is not None:
            return cached_response

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

    # store response in a file
    if with_cache:
        response_cache.set(REST_params, json_response)

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
    get bucket object if it exists
    else, return None
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
def upload_json_to_blob(folder_prefix, json_dict, blob_name, bucket_ref,
        no_replace = False
    ):

    blob_data_str = json.dumps(
        json_dict,
        sort_keys=True,
        ensure_ascii=False
    )

    blob = bucket_ref.get_blob(blob_name)

    if blob is not None:
        return
    
    player_blob = bucket_ref.blob(blob_name)
    blob = bucket_ref.blob(f"{folder_prefix}/{blob_name}")

    blob.upload_from_string(
        blob_data_str, 
        content_type = 'application/json',
    )


def read_data_from_bucket(bucket_name, blob_name, **kwargs):
    
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
    LEAGUE_TO_LOAD = 'eq.415'

    bucket_ref = get_GCS_bucket(STD_PIPELINE_BUCKET)
    if bucket_ref is None:
        print("ERROR: Could not find bucket")
        raise ValueError(f"Could not find bucket {STD_PIPELINE_BUCKET}")


    # send requests to get leagues
    api_key = os.getenv("SPORT_DEVS_API_KEY")
    league_rest_params = {
        "ENDPOINT":"leagues",
        "payload":{
            'class_id':LEAGUE_TO_LOAD, # ATP
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
    retrieve players based on league
"""
def load_players_from_leagues(seasons_lst):
    players_lst = []
    api_key = os.getenv("SPORT_DEVS_API_KEY")

    for league_id in seasons_lst:
        player_rest_params = {
            'ENDPOINT':'seasons-by-league',
            'URL':'https://tennis.sportdevs.com/',
            'headers':{
                'Accept':'application/json',
                'Authorization': 'Bearer '+api_key
            },
            'payload':{
                'league_id':f"{league_id}", # ATP
            }
        }
        player_data = send_requests(player_rest_params)
        players_lst.append(player_data['id'])

    return players_lst




"""
    load player data from a certain year.
"""
# TODO: CHANGE OF PLANS, just load players from standings/rankings. 
# finding through leagues->seasons->matches/attendees->...
# loads unnecessary data.
def load_all_players_from_year(query_year = 2025):

    NUM_LEAGUES_TO_CHECK = 50
    

    bucket_ref = get_GCS_bucket(STD_PIPELINE_BUCKET)
    if not bucket_ref:
        print("ERROR: bucket could not be found")
        raise ValueError("Bucket could not be found")
    
    leagues_checked = 0
    limit = 50
    offset = 0
    valid_leagues = []
    valid_seasons = []
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
                'offset':offset
            }
        }
        leagues_data = send_requests(leagues_rest_params)
        print("sent and retrieved leagues data")

        # retrieve season data for each league, and check if valid
        leagues_added = 0
        for league in leagues_data:
            league_id = league['id']
            # get season info
            season_rest_params = {
                'ENDPOINT': 'seasons',
                'URL':'https://tennis.sportdevs.com/seasons',
                'headers':{
                    'Accept':'application/json',
                    'Authorization': 'Bearer '+api_key
                },
                'payload':{
                    'league_id': f"eq.{league_id}",
                    'limit': limit,
                    'offset': offset,
                }
            }
            season_data = send_requests(season_rest_params)

            # check if league took place in query year
            for season in season_data:
                if season['year'] == query_year:
                    leagues_added+=1
                    valid_seasons.append(season['id'])

                    valid_leagues.append(league_id)
                    break

        leagues_checked += leagues_added
    
    players_lst = load_players_from_leagues(valid_seasons)
    return players_lst

"""
    load player data from rankings
"""
# TODO: add exception/error handling
def load_players(players_to_load = 50):
    
    players_loaded = 0
    while players_loaded < players_to_load:
        limit = min(50, players_to_load - players_loaded)

        api_key = os.getenv("SPORT_DEVS_API_KEY")
        player_rest_params = {
            'ENDPOINT':'rankings',
            'URL':'https://tennis.sportdevs.com/',
            'headers':{
                'Accept':'application/json',
                'Authorization': 'Bearer '+api_key
            },
            'payload':{
                'type':'eq.atp', # ATP
                'class':'eq.official',
                'limit':limit,
                'offset':players_loaded,
            }
        }

        player_data = send_requests(player_rest_params)
        
        # upload all players to gcs bucket
        bucket_ref = get_GCS_bucket(STD_PIPELINE_BUCKET)
        FOLDER_PREFIX = "players"

        for player in player_data:

            PLAYER_BLOB_NAME = f"{FOLDER_PREFIX}/{player['team_id']}"

            # check if bucket exists
            player_blob = bucket_ref.get_blob(PLAYER_BLOB_NAME)
            if player_blob is None:
                player_blob = bucket_ref.blob(PLAYER_BLOB_NAME)
            
            player_json_str = json.dumps(player, 
                                        sort_keys=True, 
                                        ensure_ascii = False
            )
            player_blob.upload_from_string(player_json_str, 
                                        content_type = 'application/json')
            
            players_loaded +=1



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

    load_players()
    # sending_test()





    # load_leagues()
    # retrieve_leagues()
    

    # ranking_data = send_requests(REST_param_dict)


    # engine = setup_postgres_db()

    # TODO: add exception handling to upload code

    # TODO: setup GCS buckets with Terraform
    

main()
# TODO: add exception handling
# TODO: setup GCS buckets with Terraform
# TODO: add parallelism via futures/coroutines for sending/uploading
# maybe through google cloud
# TODO: 