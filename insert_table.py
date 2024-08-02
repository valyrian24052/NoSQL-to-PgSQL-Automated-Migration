from sqlalchemy import create_engine

from Collection_to_df import getCollectionFromMongo
from handle import handleDatetime
from handle import handleInt

conn_str = 'postgresql://superset:superset@64.227.148.158:5432/dump'
def push_to_db(table, df):
    engine = create_engine(conn_str)
    try:
        # print(df.dtypes)
        df.to_sql(table, engine, if_exists='append', index=False)
        print(f"Total entries in {table}: {df.shape[0]}")
    finally:
        engine.dispose()

"""""
func to insert main tables

"""

def insert_main(table):
    get_table_main(table)

    
def get_table_main(table):
    """
    This function retrieves data from a MongoDB collection, transforms it, and inserts it into a PostgreSQL database.

    Parameters:
    table (str): The name of the MongoDB collection to retrieve data from.

    Returns:
    None
    """
    df = getCollectionFromMongo(table)

    if df.empty:
        print(f"No new data in {table} table")
        return
    
    df.columns = df.columns.str.lower().str.strip()

    date_columns = ["actualdeploymentdate",
    "planneddeploymentdate",
    "plannedstartdate",
    "actualstartdate",
    "plannedenddate",
    "actualenddate",
    'createdat', 'updatedat', 'plannedstartdate',
      'plannedenddate', 'closeddate',
        'planneddeploy_date', 'planned_end', 'planned_start',
          'accesstokencreatedat', 'loggedinat', 'refreshtokencreatedat',
            'actual_start','actual_end', 'createdon', 'uploadedat', 'deploy_date', 'actualstartdate','agentendtime','agentstarttime']

    
    columns_to_drop = ['__v', 'description','files']
    df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)

    for column_name in df.columns:
        if column_name in date_columns:
            try:
                df[column_name] = handleDatetime(df[column_name])
                # print(f"Transformed {column_name} to datetime")
            except Exception as e:
                print(f"Error transforming {column_name} to datetime: {e}")
        elif column_name in ['actualeffort','estimatedeffort','est_eff', 'act_eff','rem_eff', 'createdby', 'updatedby'] or 'id' in column_name:
            try:
                df[column_name] = handleInt(df[column_name])
                # print(f"Transformed {column_name} to int")
            except Exception as e:
                print(f"Error transforming {column_name} to int: {e}")
        elif df[column_name].apply(type).eq(dict).any():
            try:
                df[column_name] = df[column_name].apply(lambda x: str(x) if isinstance(x, dict) else x)
                # print(f"Transformed dictionary in {column_name} to string")
            except Exception as e:
                print(f"Error transforming dictionary in {column_name} to string: {e}")


    object_columns = df.select_dtypes(include=['object']).columns
    for column_name in object_columns:
        df[column_name] = df[column_name].astype(str)
        # print(f"Transformed {column_name} to string")

    
    if 'id' in df.columns:
        if table=='customer':
            df=df.drop(columns=['customerid'])
            # print('dropped empty customerid from customer table ')

        df.rename(columns={'id': f'{table}id'}, inplace=True)
        # print(f"Renamed 'id' column to '{table}id'")
    
    push_to_db(table,df)

"""
insert function to insert relation tables 

"""

def insert_rel(table, column_name, pg_table, rel_col_name):
    df = getCollectionFromMongo(table)
    if df.empty:
        print(f"No new data in {pg_table} table")
        return
    
    columns = [table + "id", "customerid", rel_col_name]
    df.columns = df.columns.str.lower()
    df.rename(columns={column_name: rel_col_name}, inplace=True)
    if 'id' in df.columns:
        df.rename(columns={'id': f'{table}id'}, inplace=True)
    df = df[columns]
    df = df.explode(rel_col_name)
    for col in columns:
        df[col] = handleInt(df[col])
    
    push_to_db(pg_table,df)