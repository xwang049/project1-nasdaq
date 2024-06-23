import nasdaqdatalink as nd
from prefect_multiprocess.task_runners import MultiprocessTaskRunner
from datetime import datetime
import time
from pytz import timezone
from prefect import task, flow, serve
from store_data import retrieve_data
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import sl_dict
import logging

load_dotenv()


MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DB')
nd.read_key(filename="mykey")


DATABASE_URL = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"

engine = create_engine(DATABASE_URL)

def get_existing_data(table_name):
    engine = create_engine(DATABASE_URL)
    query = f"SELECT * FROM {table_name}"
    existing_data = pd.read_sql(query, engine)
    return existing_data


def find_new_rows(existing_data, new_data):
    if new_data is None:
        return None
    if existing_data is None:
        return new_data
    new_rows = new_data[~new_data.apply(tuple,1).isin(existing_data.apply(tuple,1))]
    return new_rows


def insert_new_rows(table_name, new_rows):
    engine = create_engine(DATABASE_URL)
    if new_rows is None:
        return
    if new_rows.empty:
        return
    new_rows.to_sql(table_name, engine, if_exists='append', index=False)
                   
@task
def get_latest_data(table_code):
    edt = timezone('US/Eastern')
    current_time_edt = datetime.now(edt)
    previous_day = current_time_edt - timedelta(days=2)
    date_str = previous_day.strftime('%Y-%m-%d')
    try:
        print('try0')
        table, data = nd.get_table(table_code, paginate = True,  **{'date': {'gte': date_str}}) # download directly from column 'date' 
    except:
        try:
            print('try1')
            df = nd.get_table(table_code).head()
            date_str = current_time_edt.strftime('%Y-%m-%d')
            date_guess = [col for col in df.columns if 'date' in col.lower()][0]
            table, data = nd.get_table(table_code, paginate = True,  **{date_guess: {'gte': date_str}}) # In case if 'date' does not exist, we guess a filter that represents datetime
        except:
            try:
                print('try2')
                data = nd.get_table(table_code)  # other cases, we simply download the latest 10000 rows of data 
                existing_data = get_existing_data(engine, table_code.replace('/', '_').lower())
                data = find_new_rows(existing_data, data)
            except:
                return None
    print('completed')
    return data

@task(retries = 2)
def process_data(table_code):
    print(table_code)
    data = get_latest_data(table_code)
    code = table_code.replace('/', '_').lower()
    print('getdata&code')
    existing_data = get_existing_data(code)
    print('getexistingdata')
    data = find_new_rows(existing_data, data)
    print('findnewdata')
    insert_new_rows(code, data)
    print('completed')

@flow(log_prints=True)
def update_data(key: str = ''):
    process_data(key)

@flow(task_runner=MultiprocessTaskRunner(processes=8), log_prints=True)
def update_all_data(d = sl_dict.load('info_dict.pkl')):
    start_time = time.time()
    tasks = []
    temp = retrieve_data("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()").values.tolist()
    all_tables = [row[0].replace('_', '/').upper() for row in temp]
    for key in all_tables:
        if not d[key]['premium']:
            tasks.append(process_data.submit(key))
    for task in tasks:
        task.result() 
    end_time = time.time()
    logging.info(f"Total time taken: {end_time - start_time:.2f} seconds")
    
if __name__ == '__main__':
    d = sl_dict.load('info_dict.pkl')
    temp = retrieve_data("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()").values.tolist()
    all_tables = [row[0].replace('_', '/').upper() for row in temp]
    tasks = []
    for key in all_tables:
        cron = d[key]['status']['expected_at']
        if cron is None:
            cron = '00 00 * * *'         # If it is not clear when to update, we set the update time as 12:00AM everyday
        try:
            tasks.append(update_data.to_deployment(name=f"updating {key.replace('/', '_').lower()}",cron = cron, parameters = {'key' : key}))
        except:
            tasks.append(update_data.to_deployment(name=f"updating {key.replace('/', '_').lower()}",cron = '00 00 * * *', parameters = {'key' : key}))
    serve(*tasks)
