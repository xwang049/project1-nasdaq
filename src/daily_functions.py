import gzip
import os
import shutil
import nasdaqdatalink as nd
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from .config import DATABASE_URL, NASDAQ_API
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
def save_to_db(dataframe, table_name, if_exists='replace'):
    engine = create_engine(DATABASE_URL)
    try:
        table_name = table_name.replace('/', '_').lower()
        dataframe.to_sql(table_name, engine, if_exists=if_exists, index=False)
        logging.info(f"Data saved to {table_name} table successfully.")
    except SQLAlchemyError as e:
        logging.error(f"Error saving data to database: {str(e)}")


@task(retries=3)
def save_daily_update_to_db(dataframe, table_name, date_column):
    engine = create_engine(DATABASE_URL)
    nd.ApiConfig.api_key = NASDAQ_API
    try:
        table_name = table_name.replace('/', '_').lower()
        date_values = dataframe[date_column].unique()

        for date in date_values:
            delete_query = f"DELETE FROM {table_name} WHERE {date_column} = '{date}'"
            with engine.connect() as connection:
                connection.execute(delete_query)

        dataframe.to_sql(table_name, engine, if_exists='append', index=False)
        logging.info(f"Daily update saved to {table_name} table successfully.")
    except SQLAlchemyError as e:
        logging.error(f"Error saving daily update to database: {str(e)}")


@task(retries=3)
def get_local_table_latest_timestamp(table_name):
    engine = create_engine(DATABASE_URL)
    try:
        table_name = table_name.replace('/', '_').lower()
        query = f"SELECT MAX(date) AS max_date FROM {table_name}"
        result = pd.read_sql(query, engine)
        return result.iloc[0, 0]
    except SQLAlchemyError as e:
        logging.error(f"Error retrieving latest timestamp from {table_name}: {str(e)}")
        return None


@task(retries=3)
def process_daily_update(key):
    nd.ApiConfig.api_key = NASDAQ_API
    try:

        df = nd.get_table(key).head()
        date_guess = [col for col in df.columns if 'date' in col.lower()][0]
        latest_timestamp = get_local_table_latest_timestamp(key)

        if latest_timestamp:
            table, data = download_data(key, **{date_guess: {'gte': latest_timestamp}})
        else:
            table, data = download_data(key)

        if data is not None and not data.empty:
            save_daily_update_to_db(data, key, date_guess)
    except Exception as e:
        logging.error(f"Error processing daily update for {key}: {str(e)}")