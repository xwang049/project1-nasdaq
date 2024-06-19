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
def download_all_tables(d):
    start_time = time.time()
    num_processes = 8
    print(d.keys(), '+', NASDAQ_API)
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(process_data, key) for key in d.keys() if d[key]['premium'] == False]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error: {str(e)}")
    end_time = time.time()
    logging.info(f"Total time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    d = load('../data/info_dict.pkl')
    download_all_tables(d)