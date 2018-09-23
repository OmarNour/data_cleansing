import datetime
import hashlib
import json
import pymongo
import dask
import pandas as pd
import data_cleansing.CONFIG.Config as DNXConfig
import sys


# @dask.delayed
def sha1(value):
    if type(value) != 'str':
        value = str(value)
    hash_object = hashlib.sha1(value.encode())
    hex_dig = hash_object.hexdigest()
    return str(hex_dig)


def assign_process_no(no_of_cores, index):
    process_no = index % no_of_cores
    # print(process_no)
    return process_no

# @dask.delayed
def insert_into_collection(mongo_uri, mongo_db, collection_name, data_dict):
    client = pymongo.MongoClient(mongo_uri)
    database = client[mongo_db]
    # print(data_dict)
    if data_dict:
        # print(database, collection_name)
        insert_db = database[collection_name].insert_many(data_dict
                                                          , ordered=False
                                                          , bypass_document_validation=True
                                                          )

        return insert_db.inserted_ids
    else:
        return []


def delete_from_collection_where_ids(mongo_uri, mongo_db, collection_name, ids):
    client = pymongo.MongoClient(mongo_uri)
    database = client[mongo_db]
    if collection_exists(database, collection_name):
        database[collection_name].delete_many({'_id': {'$in': ids}})


def collection_exists(database, collection_name):
    if collection_name in database.collection_names():
        return True
    else:
        return False


def get_sub_data(mongo_uri, mongo_db, collection_name,start_index, end_index, ids=None, collection_filter=None, collection_project=None):
    client = pymongo.MongoClient(mongo_uri)
    database = client[mongo_db]
    if ids is None:
        if collection_filter is None:
            collection_filter = {}
        if collection_project is None:
            collection_data = database[collection_name].find(collection_filter, no_cursor_timeout=True).skip(start_index).limit(end_index-start_index)
        else:
            collection_data = database[collection_name].find(collection_filter, collection_project, no_cursor_timeout=True).skip(start_index).limit(end_index-start_index)

        # collection_data = collection_data[start_index: end_index]
    # else:
    #     collection_data = database[collection_name].find({'_id': {'$in': ids}})
    return collection_data


def data_to_list(data):

    # print('size of datadata ',sys.getsizeof(data))
    list_data = list(data)
    return list_data


def df_to_dict(df):
    if not df.empty:
        data_dict = df.to_dict('records')
    else:
        data_dict = {}
    return data_dict


def chunk_list(list_data,chunk_size):
    chunks = (list_data[x:x + chunk_size] for x in
              range(0, len(list_data), chunk_size))
    return chunks


def integer_rep(string):
    string_int = [ord(c) for c in string]
    string_int = "".join(str(n) for n in string_int)
    string_int = int(string_int)
    return string_int


def chunk_list_loop(list_data,chunk_size):
    for chunk in chunk_list(list_data,chunk_size):
        yield chunk


def get_start_end_index(total_rows, chunk_size):
    start_index = 0
    end_index = min(chunk_size, total_rows)
    max_index = total_rows
    while start_index < end_index:
        yield start_index, end_index
        start_index = min(end_index, max_index)
        end_index = min(end_index + chunk_size, max_index)


def get_minimum_category(be_att_id):
    client = pymongo.MongoClient(DNXConfig.Config.mongo_uri)
    config_database = client[DNXConfig.Config.config_db_name]
    # print('be_minimum_category')
    be_minimum_category = config_database[DNXConfig.Config.be_attributes_data_rules_collection].aggregate([{'$match': {'be_att_id': be_att_id}},
                                                                                                           {'$group': {'_id': '$be_att_id',
                                                                                                                       'min_cat': {
                                                                                                                           '$min': '$category_no'}}}])
    # return 1
    # print('be_minimum_category', be_minimum_category)
    for i in be_minimum_category:
        # print('min_cat', int(i['min_cat']))
        return int(i['min_cat'])
    return 1


def get_parameter_values(mongo_uri, config_db, parameter_collection_name):
    client = pymongo.MongoClient(mongo_uri)
    config_database = client[config_db]
    try:
        return config_database[parameter_collection_name].find()
    except:
        return None
