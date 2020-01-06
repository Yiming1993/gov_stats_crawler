import config
from bson import ObjectId
import numpy as np


def simple_count(db_name, collection_name, condition, keyword_name):
    _db = config.db_path(db_name)
    data = _db[collection_name].find(condition)
    data_dict = {}
    for doc in data:
        name = doc[keyword_name]
        if name not in data_dict:
            data_dict[name] = 1
        else:
            data_dict[name] = 1 + data_dict[name]
    return data_dict

def count_with_rule(db_name, collection_name, condition, keyword_name, keyword_value_name):
    _db = config.db_path(db_name)
    data = _db[collection_name].find(condition)
    data_dict = {}
    for doc in data:
        name = doc[keyword_name]
        value = doc[keyword_value_name]
        if name not in data_dict:
            data_dict[name] = value
        else:
            data_dict[name] = value + data_dict[name]

    return data_dict

def count(data_list):
    return data_list[0]

def weight_count(data_list):
    return data_list[0] * data_list[1]

def save_data(db_name, collection_name, title, data):
    data["title"] = title
    _db = config.db_path(db_name)
    _db[collection_name].insert(data)
    print('data {} is saved'.format(title))

if __name__ == '__main__':
    data_dict = count_with_rule('')
    save_data('',data_dict)