import time
import logging
from concurrent.futures import ProcessPoolExecutor
from src.config import NASDAQ_API
# from src import sl_dict
from src.sl_dict import load, save
from src.store_data import process_data
import concurrent
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# TODO: collect download error sets and rerun them.
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

if __name__ == "__main__":
    d = load('../data/info_dict.pkl')
    download_all_tables(d)
