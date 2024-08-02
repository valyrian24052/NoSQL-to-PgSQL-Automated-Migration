from sqlalchemy import create_engine

from Resources import handleInt,handleDatetime,getCollectionFromMongo
from input import Mongoconnstr

conn_str = Mongoconnstr


def insert_main(table,schema):
    
    df = preprocess_data(table,schema)
    push_to_db(table, df)
    

def push_to_db(table, df):
    engine = create_engine(conn_str)
    try:
        print(df.dtypes)
        df.to_sql(table, engine, if_exists='append', index=False)
        print(f"Total entries in {table}: {df.shape[0]}")
    finally:
        engine.dispose()


def preprocess_data(table, schema):
    df = getCollectionFromMongo(table)

    if df.empty:
        print(f"No data in {table} table")
        return
    
    df.columns = df.columns.str.lower().str.strip()
    
    type_handlers = {
        'integer': handleInt,
        'datetime': handleDatetime,
    }

    for column_name, dtype in schema:
        if column_name in df.columns:
            try:
                if dtype in type_handlers:
                    df[column_name] = type_handlers[dtype](df[column_name])
                elif dtype == 'string':
                    df[column_name] = df[column_name].astype(str)
            except Exception as e:
                print(f"Error transforming {column_name} to {dtype}: {e}")
    
