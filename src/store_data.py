from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import SQLAlchemyError
from .config import DATABASE_URL
import logging
from .download_data import download_data
engine = create_engine(DATABASE_URL)

def save_to_db(dataframe, table_name, engine):
    try:
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Table {table_name} created or replaced and data saved successfully.")
    except SQLAlchemyError as e:
        logging.error(f"Error saving {table_name}: {str(e)}")

def download_and_store_data(table, filters, engine):
    table_name, data = download_data(table, **filters)
    if data is not None:
        table_name = table_name.replace('/', '_').lower()
        save_to_db(data, table_name, engine)
    else:
        logging.error(f"Failed to download data for table: {table}")
