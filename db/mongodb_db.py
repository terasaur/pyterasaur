#
# Copyright 2012 ibiblio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0.txt
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pymongo
import copy
from terasaur.config.config_helper import MONGODB_SECTION
from bson.objectid import ObjectId

__DB_CONN = None
DEFAULT_DB_PARAMS = None
DB_PARAMS = None

class MongoDbException(Exception): pass

def reset_config_values(default_values):
    global DEFAULT_DB_PARAMS
    global DB_PARAMS
    DEFAULT_DB_PARAMS = copy.deepcopy(default_values[MONGODB_SECTION])
    DEFAULT_DB_PARAMS['db_port'] = int(DEFAULT_DB_PARAMS['db_port'])
    DB_PARAMS = copy.deepcopy(DEFAULT_DB_PARAMS)

def set_connection_params(**kwargs):
    global DB_PARAMS
    for key in kwargs:
        if DB_PARAMS.has_key(key):
            DB_PARAMS[key] = kwargs[key]

def set_connection_params_from_config(config):
    """
    Accept a ConfigParser object and extract connection params from
    the section named 'seedbank_db'.
    """
    if MONGODB_SECTION not in config.sections():
        raise MongoDbException('Did not find ' + MONGODB_SECTION + ' section in config')

    global DB_PARAMS
    for name in DB_PARAMS:
        if config.has_option(MONGODB_SECTION, name):
            if name == 'db_port':
                DB_PARAMS[name] = config.getint(MONGODB_SECTION, name)
            else:
                DB_PARAMS[name] = config.get(MONGODB_SECTION, name)

def save(collection_name, data):
    oid = _execute('save', collection_name, data)
    return ObjectId(oid)

def delete(collection_name, data):
    _execute('delete', collection_name, data)

def get(collection_name, query):
    item = _execute('get', collection_name, query)
    return item

def find(collection_name, query):
    results = _execute('find', collection_name, query)
    return results

def update(collection_name, query, data):
    # results is None unless we enable write ack
    results = _execute('update', collection_name, query, data)
    return results

def _execute(operation, coll_name, data_or_query, update_data=None):
    conn = get_db_conn()
    db = conn[DB_PARAMS['db_name']]
    results = None

    if operation == 'save':
        results = db[coll_name].save(data_or_query)
    elif operation == 'delete':
        results = db[coll_name].remove(data_or_query)
    elif operation == 'get':
        results = db[coll_name].find_one(data_or_query)
    elif operation == 'find':
        results = db[coll_name].find(data_or_query)
    elif operation == 'update':
        results = db[coll_name].update(data_or_query, update_data, upsert=False)
    else:
        raise MongoDbException('Invalid operation: ' + operation)

    conn.end_request()
    return results

def get_db_conn():
    global __DB_CONN
    if __DB_CONN is None:
        __DB_CONN = pymongo.Connection(DB_PARAMS['db_host'], DB_PARAMS['db_port'], tz_aware=True)
    return __DB_CONN

def close():
    global __DB_CONN
    if __DB_CONN is not None:
        __DB_CONN.disconnect()
        __DB_CONN = None

def bool_to_mongo(val):
    """
    Convert python boolean value to pymongo-appropriate string
    representation ('1' or '0').  Pymongo doesn't seem to handle boolean
    values properly.
    """
    return '1' if val is True else '0'

def mongo_to_bool(val):
    """
    Convert pymongo boolean string to python boolean value.
    """
    return True if val == '1' else False
