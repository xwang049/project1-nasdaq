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
engine = create_engine(DATABASE_URL)

def compress_file(file_path):
    with open(file_path, 'rb') as f_in:
        with gzip.open(file_path + '.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(file_path)

def save_to_db(dataframe, table_name):
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

def process_data(key):
    try:
        df = nd.get_table(key).head()
        date_guess = [col for col in df.columns if 'date' in col.lower()][0]
        table, data = download_data(key, paginate = True,  **{date_guess: {'gte': '2024-03-15'}})
    except:
        table, data = download_data(key, paginate = True)
    save_to_db(data, key)
