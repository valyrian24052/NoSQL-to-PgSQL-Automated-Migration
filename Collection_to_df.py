from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta

current_time = datetime.now()
end_time = current_time - timedelta(minutes=8)
Start_time = end_time - timedelta(minutes=1440)


def getCollectionFromMongo(collection_name, start_time=Start_time, end_time=end_time):
    uri = "mongodb://temp:temp123@164.52.218.240:27017/qa"
    client = MongoClient(uri)
    db = client['qa']
    collection = db[collection_name]
    query = {
        '$or': [
            {'createdAt': {'$gte': start_time, '$lt': end_time}},
            {'updatedAt': {'$gte': start_time, '$lt': end_time}}
        ]
    }

    cursor = collection.find(query)
    df = pd.DataFrame(cursor)
    client.close()
    return df