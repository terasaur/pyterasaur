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

import terasaur.db.mongodb_db as mongodb_db

"""
Customized interface for Torrent CRUD functions
"""

TORRENT_COLLECTION = 'torrent'

def get(info_hash):
    item = mongodb_db.get(TORRENT_COLLECTION, {'info_hash': info_hash})
    return item

def save(torrent_data):
    oid = mongodb_db.save(TORRENT_COLLECTION, torrent_data)
    return oid

def delete(info_hash):
    mongodb_db.delete(TORRENT_COLLECTION, {'info_hash': info_hash})

def find(query=None):
    results = mongodb_db.find(TORRENT_COLLECTION, query)
    return results

def update(query, data):
    results = mongodb_db.update(TORRENT_COLLECTION, query, data)
    return results

def initialize():
    conn = mongodb_db.get_db_conn()
    db = conn[mongodb_db.DB_PARAMS['db_name']]
    db[TORRENT_COLLECTION].ensure_index('info_hash', unique=True)
    conn.end_request()
