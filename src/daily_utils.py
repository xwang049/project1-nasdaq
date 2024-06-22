import time
import logging
from concurrent.futures import ProcessPoolExecutor
from src.config import NASDAQ_API
from src.sl_dict import load, save
from src.daily_functions import process_daily_update
from src.store_data import process_data
import concurrent
from prefect import task, flow
from prefect_multiprocess.task_runners import MultiprocessTaskRunner

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@flow(task_runner=MultiprocessTaskRunner(processes=6), log_prints=True)
def download_all_tables(d):
    start_time = time.time()
    tasks = []
    for key in d.keys():
        if not d[key]['premium']:
            tasks.append(process_data.submit(key))
    for task in tasks:
        task.result()
    end_time = time.time()
    logging.info(f"Total time taken: {end_time - start_time:.2f} seconds")

@flow(task_runner=MultiprocessTaskRunner(processes=6), log_prints=True)
def download_daily_updates(d):
    start_time = time.time()
    tasks = []
    for key in d.keys():
        if not d[key]['premium']:
            tasks.append(process_daily_update.submit(key))
    for task in tasks:
        task.result()
    end_time = time.time()
    logging.info(f"Total time taken for daily updates: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    d = load('../data/info_dict.pkl')
    download_all_tables(d)
    # download_daily_updates(d)
