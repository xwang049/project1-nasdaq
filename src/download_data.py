import nasdaqdatalink as nd
from src.config import NASDAQ_API
from prefect import task, flow

def download_data(table, **filters):
    nd.ApiConfig.api_key = NASDAQ_API
    try:
        data = nd.get_table(table, **filters)
        return table, data
    except Exception as e:
        print(f"Error downloading data for {table}: {str(e)}")
        return table, None
