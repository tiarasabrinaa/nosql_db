from pymongo import MongoClient

def connect_mongodb(uri, db_name):
    client = MongoClient(uri)
    db = client[db_name]
    return db