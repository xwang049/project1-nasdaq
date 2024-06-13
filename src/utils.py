import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .store_data import download_and_store_data, engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_all_tables(tables_filters):
    start_time = time.time()
    num_workers = len(tables_filters)
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_table = {executor.submit(download_and_store_data, table, filters, engine): table for table, filters in tables_filters.items()}
        for future in as_completed(future_to_table):
            table = future_to_table[future]
            try:
                future.result()
                logging.info(f"Successfully processed table {table}")
            except Exception as e:
                logging.error(f"Error processing table {table}: {str(e)}")
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
