import json
import os.path
import hashlib

from collections.abc import Mapping

def stable_hash(str_obj):
    return hashlib.md5(str_obj.encode('utf-8')).hexdigest()


def serialize_params(obj):
    if obj is None:
        return None
    
    if isinstance(obj, Mapping):

        serials_lst = []
        for k, v in obj.items():
            key_component = str(k)  # key is conveniently always a str
            val_component = serialize_params(v)

            serialized_map = (key_component, val_component)
            serials_lst.append(serialized_map)

        return tuple(sorted(serials_lst))
        

    else:
        return obj
    
def set(params, response):
    file_name = None
    try:
        
        file_name = str(serialize_params(params))
        file_name = stable_hash(file_name)
    except:
        print("Failed to serialize params.")
        return False

    with open(f"./data/{file_name}.json", 'w') as file:
        json.dump(response, file, 
                  indent = 4, 
                  sort_keys = True, 
                  ensure_ascii = False)

    return True


def get(params):
    file_name = None
    try:
        file_name = str(serialize_params(params))
        file_name = stable_hash(file_name)
        print(file_name)
    except:
        print("Failed to serialize params.")
        return None
    
    # check if file exists
    path_str = f"./data/{file_name}.json"
    if not os.path.isfile(path_str):
        print("Could not find file.")
        return None
    
    with open(path_str, 'r') as file_data:
        json_data = json.load(file_data)

        return json_data
api_key = "1234"
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

# test set
print(stable_hash(str(serialize_params(league_rest_params))))
with open('./store_leagues.json', 'r') as f:
    response = json.load(f)
    set(league_rest_params, response)

# test get
print(type(get(league_rest_params)))