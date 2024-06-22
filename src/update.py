
import time
import logging
import nasdaqdatalink as nd
from src.config import NASDAQ_API
from src.sl_dict import load
from prefect_multiprocess.task_runners import MultiprocessTaskRunner
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from sqlalchemy import create_engine, text
from config import DATABASE_URL
from sqlalchemy.engine import Engine
from prefect import task, flow
from datetime import datetime
import logging
from src.download_data import download_data
from src.store_data import save_to_db


def get_db_engine() -> Engine:
    """ENGINE CONNECTION"""
    return create_engine(DATABASE_URL)


def get_date_column(key):
    nd.ApiConfig.api_key = NASDAQ_API
    df = nd.get_table(key).head()
    if 'date' in df.columns:
        return 'date'

    date_guess = [col for col in df.columns if 'date' in col.lower()]
    if not date_guess:
        raise ValueError(f"No date-like column found in the table {key}")
    return date_guess[0]

def delete_existing_data(engine: Engine, table_name: str, date_column: str, date: str):
    """DELETE one day data, run it before update the latest sets"""
    with engine.connect() as connection:
        delete_query = text(f"DELETE FROM {table_name} WHERE {date_column} = :date")
        connection.execute(delete_query, {'date': date})
        logging.info(f"Deleted old data from {table_name} on {date}.")


@task(retries = 3)
def update_data(key):
    try:
        now = datetime.utcnow()
        today = now.strftime('%Y-%m-%d')
        date_guess = get_date_column(key)
        table, new_data = download_data(key, paginate=True, **{date_guess: {'2024-06-21'}})
        if new_data is None or new_data.empty:
            logging.info(f"No new data for {key} on {today}.")
            return

        engine = get_db_engine()
        table_name = key.replace('/', '_').lower()
        with engine.connect() as connection:
            delete_query = text(f"DELETE FROM {table_name} WHERE {date_guess} = :date")
            connection.execute(delete_query, {'date': '2024-06-21'})
            logging.info(f"Deleted old data for {key} on {'2024-06-21'}.")

        save_to_db(new_data, key, 'append')
    except Exception as e:
        logging.error(f"Error updating data for {key}: {e}")



@flow(task_runner=MultiprocessTaskRunner(processes=6), log_prints=True)
def update_all_tables(d):
    start_time = time.time()
    tasks = []
    for key in d.keys():
        if not d[key]['premium']:
            tasks.append(update_data.submit(key))
    for task in tasks:
        task.result()
    end_time = time.time()
    logging.info(f"Total time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    d = load('../data/info_dict.pkl')
    update_all_tables(d)