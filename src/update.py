import nasdaqdatalink as nd
from datetime import datetime
from pytz import timezone
from prefect import task, flow
import utils
import download_data
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from download_data import download_data

load_dotenv()


MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DB')
nd.read_key(filename="mykey")


DATABASE_URL = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"

engine = create_engine(DATABASE_URL)

def get_existing_data(engine, table_name):
    query = f"SELECT * FROM {table_name}"
    existing_data = pd.read_sql(query, engine)
    return existing_data

def find_new_rows(existing_data, new_data):
    new_rows = new_data[~new_data.apply(tuple,1).isin(existing_data.apply(tuple,1))]
    return new_rows

def insert_new_rows(engine, table_name, new_rows):
    new_rows.to_sql(table_name, engine, if_exists='append', index=False
                   
@task
def get_latest_data(table_code):
    edt = timezone('US/Eastern')
    current_time_edt = datetime.now(edt)
    previous_day = current_time_edt - timedelta(days=2)
    date_str = previous_day.strftime('%Y-%m-%d')
    try:
        table, data = download_data.submit('NDAQ/RTAT10', paginate = True,  **{'date': {'gte': date_str}}).result() # download directly from column 'date' 
    except:
        try:
            print('try1')
            df = nd.get_table(table_code).head()
            date_str = current_time_edt.strftime('%Y-%m-%d')
            date_guess = [col for col in df.columns if 'date' in col.lower()][0]
            table, data = download_data.submit(table_code, paginate = True,  **{date_guess: {'gte': date_str}}).result() # In case if 'date' does not exist, we guess a filter that represents datetime
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

@task(retries = 3)
def process_data(table_code):
    data = get_latest_data(table_code)
    code = table_code.replace('/', '_').lower()
    existing_data = get_existing_data(engine, code)
    newdata = find_new_rows(existing_data, data)
    insert_new_rows(engine, code, newdata) # insert the new rows by comparing them with the previous data

if __name__ == '__main__':
    process_data('NDAQ/RTAT10')
