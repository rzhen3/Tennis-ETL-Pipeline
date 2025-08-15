import json
import os.path

from collections.abc import Mapping


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
        
        file_name = serialize_params(params)
    except:
        print("Failed to serialize params.")
        return False

    with open(f"./data/{file_name}", 'w') as file:
        json.dump(response, file)

    return True


def get(params):
    file_name = None
    try:
        file_name = serialize_params(params)
    except:
        print("Failed to serialize params.")
        return None
    
    # check if file exists
    path_str = f"./data/{file_name}.json"
    if not os.path.isfile(path_str):
        print("Could not find file.")
        return None
    
    
    

d = {
    "payload":{
        "limit":"10",
        "class_id":30
    },"ENDPOINT":"leagues",
    
}
print(compute_hash_rec(d))
# for k, v in d.items():
#     x = sorted((str(k), v))
#     print(x)

# return tuple(sorted(
#     (str(k), compute_hash_rec(v)) for k, v in obj.items()
# ))
