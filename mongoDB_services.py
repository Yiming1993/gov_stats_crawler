from pymongo import MongoClient
from bson import ObjectId

def connect_db(host, port, user_name, password, db_name):
    host = host
    port = port
    user_name = user_name
    user_pwd = password
    db_name = db_name
    uri = "mongodb://" + user_name + ":" + user_pwd + "@" + host + ":" + port + "/" + db_name
    client = MongoClient(uri, connect=True)
    db = client[db_name]
    return db

def get_data(db, collection_name, find_rule, data_tag):
    data_list = []
    data = db[collection_name].find(find_rule)
    for doc in data:
        if type(data_tag) == type(str):
            data_list.append(doc[data_tag])
        if type(data_tag) == type(list):
            single_data = []
            for tag in data_tag:
                single_data.append(doc[tag])
            data_list.append(single_data)
    return data_list

def save_data(db, collection_name, data, exist_detect_tag = 'name', exist_detect = True):
    if exist_detect == True:
        exist = db[collection_name].find({exist_detect_tag: data[exist_detect_tag]}).count()
        if exist == 0:
            db[collection_name].insert(data)
            print('data {} is saved'.format(data[exist_detect_tag]))
        else:
            print('data {} exists'.format(data[exist_detect_tag]))
    else:
        db[collection_name].insert(data)
        print('data {} is saved'.format(data[exist_detect_tag]))

def update_data(db, collection_name, id, data):
    db[collection_name].update({ObjectId(id)},{"$set":data}, True, True)
    print('data {} is updated with data {}'.format(id, data))