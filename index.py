from appdynamics.agent import api as appd

from pymongo import MongoClient
from bson.json_util import dumps
from bson.objectid import ObjectId

client = MongoClient('mongodb://localhost:27017/')
# "myproject", is my database name
db = client.myproject

APPD_BT_HANDLE = None
APPD_EXIT_CALL_HANDLE = None
def appd_bt_wrapper(transaction_name):
    def inner_wrapper(func):
        def wrapper(*args, **kwargs):
            global APPD_BT_HANDLE
            correlation_header = None
            bt_handle = appd.start_bt(transaction_name, correlation_header=correlation_header)
            APPD_BT_HANDLE = bt_handle
            
            ret = func(*args, **kwargs)
            appd.end_bt(bt_handle)
            APPD_BT_HANDLE = None
            return ret
        return wrapper
    return inner_wrapper

def appd_exit_call_wrapper(exit_type, exit_subtype, display_name, identifying_properties, operation):
    def inner_wrapper(func):
        def wrapper(*args, **kwargs):
            global APPD_BT_HANDLE
            global APPD_EXIT_CALL_HANDLE
            bt_handle = APPD_BT_HANDLE
            APPD_EXIT_CALL_HANDLE = bt_handle
            with appd.exit_call(
                bt_handle=bt_handle,
                exit_type=exit_type,
                exit_subtype=exit_subtype,
                display_name=display_name,
                identifying_properties=identifying_properties,
                operation=operation
            ) as exit_handle:
                ret = func(*args, **kwargs)
            return ret
        return wrapper
    return inner_wrapper

@appd_exit_call_wrapper(
    exit_type=appd.EXIT_CUSTOM,
    exit_subtype=appd.EXIT_SUBTYPE_MONGODB,
    display_name='My Mongo',
    identifying_properties={'Host': 'localhost', 'Port': 0000, 'Vendor': 'my db'},
    operation='My Transaction')
def exit_handler():
    # "documents", is my collection name
    # my sample collection  - {"a": 1, "_id": {"$oid": "5cd2596cdd56e9c98a84d154"}}
    
    # find a record
    doc = db.documents
    data = doc.find({'_id': ObjectId('5cd2596cdd56e9c98a84d154')})
    # Update a record
    import random
    value = random.randint(1,101)

    query = {'_id': ObjectId('5cd2596cdd56e9c98a84d154')}
    newvalues = {'$set': {'a': value}}
    doc.update(query, newvalues)
    return data

@appd_bt_wrapper(transaction_name='/my_bt_name')
def start_handler(environ, start_resp):
    data = exit_handler()
    '''
        Below code is to test if concurrent transaction pollute the BT handler, APPD_BT_HANDLE.
        Tested and succeded in concurreny:  **** siege -c 50 -t 3M http://localhost:5000 ****
    '''
    '''
    maintained_bt_handler = "true"
    if APPD_BT_HANDLE != APPD_EXIT_CALL_HANDLE:
        maintained_bt_handler = "false"
    with open('somefile.txt', 'a') as the_file:
        the_file.write(str(APPD_BT_HANDLE)+' ----- '+str(APPD_EXIT_CALL_HANDLE)+' ------- '+maintained_bt_handler+'\n')
    '''
    start_resp('200 OK', [('Content-Type', 'application/json')])
    return dumps(data).encode('utf-8')

def application(environ, start_resp):
    return start_handler(environ, start_resp)
