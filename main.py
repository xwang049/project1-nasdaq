
import pandas as pd

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

import nasdaqdatalink as nd


# metadata = nd.get('NSE/OIL')
# print(metadata)

# data = nd.Dataset('WIKI/AAPL').data()
# dataset_data = nd.Dataset('WIKI/AAPL').data(params={ 'start_date':'2001-01-01', 'end_date':'2010-01-01', 'collapse':'annual', 'transformation':'rdiff', 'rows':4 })
# print(dataset_data[0].date)


# data = nd.Datatable('ZACKS/FC').data()
# data2 = nd.Datatable('ZACKS/FC').data(params={'qopts': {'cursor_id': data.meta['next_cursor_id']}})
#
# data_list = []
# cursor_id = None
# while True:
#     data = nd.Datatable('ZACKS/FC').data(params={'ticker': ['AAPL','MSFT'], 'per_end_date': {'gte': '2015-01-01'}, 'qopts': {'columns': ['ticker', 'comp_name'], 'cursor_id': cursor_id}})
#     cursor_id = data.meta['next_cursor_id']
#     data_list.append(data)
#     if cursor_id is None:
#         break
# nd.Database('ZEA').bulk_download_url()


db = nd.Database.all()
print(db)
