import nasdaqdatalink as nd

def download_data(table, **filters):
    try:
        data = nd.get_table(table, **filters)
        return table, data
    except Exception as e:
        print(f"Error downloading data for {table}: {str(e)}")
        return table, None
