import gzip
import os
import shutil
import nasdaqdatalink as nd
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from .config import DATABASE_URL
import logging
from .download_data import download_data
from prefect import task, flow
engine = create_engine(DATABASE_URL)

def compress_file(file_path):
    with open(file_path, 'rb') as f_in:
        with gzip.open(file_path + '.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(file_path)

@task(retries=3)
def save_to_db(dataframe, table_name):
    engine = create_engine(DATABASE_URL)
    try:
        table_name = table_name.replace('/', '_').lower()
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Data saved to {table_name} table successfully.")
    except SQLAlchemyError as e:
        logging.error(f"Error saving data to database: {str(e)}")
        
def retrieve_data(query):
    engine = create_engine(DATABASE_URL)
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
    return result

@task(retries=3)
def process_data(key):
    try:
        df = nd.get_table(key).head()
        date_guess = [col for col in df.columns if 'date' in col.lower()][0]
        table, data = download_data(key, paginate = True,  **{date_guess: {'gte': '2024-03-15'}})
        save_to_db(data, key)
    except:
        table, data = download_data(key, paginate = True)
        save_to_db(data, key)

def store_compressed_data_to_db(file_path, table_name):
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                data MEDIUMBLOB
            );
        """))#Make sure database columns are large enough to store compressed data

    metadata = sa.MetaData()
    compressed_table = sa.Table(table_name, metadata, autoload_with=engine)

    with open(file_path, 'rb') as f:
        compressed_data = f.read()
    print(f"Compressed data read from file: {len(compressed_data)} bytes")
    print(compressed_data)
    with engine.connect() as conn:
        insert_stmt = compressed_table.insert().values(data=compressed_data)
        result = conn.execute(insert_stmt)
        conn.commit()#used to commit the current transaction, ensuring that all changes to the database are persisted.
        record_id = result.inserted_primary_key[0]
    
    print(f"Compressed data has been stored in the {table_name} table with record ID {record_id}.")
    print(result)
    return record_id


def load_compressed_data_from_db(table_name, record_id):
    """
    Load and decompress data from the database.
    table_name (str): The name of the table where the compressed data is stored.
    record_id (int): The ID of the record to load.
    """
    engine = create_engine(DATABASE_URL)
    metadata = sa.MetaData()
    compressed_table = sa.Table(table_name, metadata, autoload_with=engine)
    
    with engine.connect() as conn:
        #Perform a raw SQL query
        query = f"SELECT data FROM {table_name} WHERE id = :record_id"
        result = conn.execute(sa.text(query), {"record_id": record_id}).fetchone()
        if result is None:
            raise ValueError(f"No record found with ID {record_id}")
        
        compressed_data = result[0]
        print(f"Compressed data length: {len(compressed_data)} bytes")  # Add debug print
    
    with gzip.open(BytesIO(compressed_data), 'rb') as f:
        df = pickle.load(f)
    
    return df
