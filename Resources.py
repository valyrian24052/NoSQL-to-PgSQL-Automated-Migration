import pandas as pd
from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime, timedelta

from input import pgconnstr, Mongoconnstr,timediff


current_time = datetime.now()
end_time = current_time 
Start_time = end_time - timedelta(minutes=timediff)


def getCollectionFromMongo(collection_name, start_time=Start_time, end_time=end_time):
    uri = pgconnstr
    client = MongoClient(uri)
    db = client['qa']
    collection = db[collection_name]
    query = {
        '$or': [
            {'createdAt': {'$gte': start_time, '$lt': end_time}},
            {'updatedAt': {'$gte': start_time, '$lt': end_time}}
        ]
    }

    cursor = collection.find()
    df = pd.DataFrame(cursor)
    client.close()
    return df


def handleInt(series):
    return pd.to_numeric(series, errors='coerce').astype(pd.Int64Dtype())


def handleDatetime(column):
    column = pd.to_datetime(column, errors='coerce', utc=True)
    column = column.dt.tz_convert(None)
    return column


def get_mongo_schema(conn_str):
    client = MongoClient(conn_str)
    
    db_name = conn_str.split('/')[-1].split('?')[0]
    db = client[db_name]
    schema_dict = {}
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        sample_docs = collection.find().limit(100)
        field_types = defaultdict(set)
        
        for doc in sample_docs:
            for field, value in doc.items():
                field_types[field].add(type(value).__name__)
        
        schema = []
        for field, types in field_types.items():
            if 'ObjectId' in types:
                dtype = 'string'
            elif 'str' in types:
                dtype = 'string'
            elif 'int' in types:
                dtype = 'integer'
            elif 'float' in types:
                dtype = 'float'
            elif 'bool' in types:
                dtype = 'boolean'
            elif 'datetime' in types:
                dtype = 'datetime'
            else:
                dtype = 'string'  

            schema.append((field, dtype))
        schema_dict[collection_name] = schema
    
    return schema_dict