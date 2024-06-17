import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .store_data import process_data, engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_all_tables(d = sl_dict.load('info_dict.pkl')):
    start_time = time.time()
    num_processes = 8
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
    tables_filters = {
        'QDL/ODA': {},
        'QDL/FON': {},
        'QDL/OPEC': {},
        'QDL/JODI': {},
        'QDL/BITFINEX': {},
        'QDL/BCHAIN': {},
        'QDL/LME': {},
        'ZILLOW/DATA': {},
        'WASDE/DATA': {},
        'WB/DATA': {}
    }
    download_all_tables(tables_filters)
