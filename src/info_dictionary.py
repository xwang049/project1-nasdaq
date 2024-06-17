import csv
import nasdaqdatalink as nd
from src import sl_dict
from src.config import NASDAQ_API

nd.ApiConfig.api_key = NASDAQ_API

def csv_to_nested_dict(file_path):
    nested_dict = {}
    nd.ApiConfig.api_key = NASDAQ_API
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        for row_number, row in enumerate(csv_reader, start=0):
            for element in row:
                if element:
                    try:
                        nested_dict[element] = {'url': url_list[row_number]}
                        datatable = nd.Datatable(element)
                        data_fields = datatable.data_fields()
                        data_list = datatable.to_list()
                        dic = dict(zip(data_fields, data_list))
                        nested_dict[element].update(dic)
                    except:
                        print('Error!' + str(element))
                        try:
                            del nested_dict[element]
                        except:
                            continue
    for key in nested_dict.keys():
        t = []
        if [nested_dict[key]['url'][0]] in url_list_f:
            t.append('Fundamentals')
        if [nested_dict[key]['url'][0]] in url_list_n:
            t.append('National Statistics')
        if [nested_dict[key]['url'][0]] in url_list_p:
            t.append('Prices & Volumes')
        if t == []:
            t.append('Others')
        nested_dict[key]['Type'] = t
    return nested_dict


if __name__ == '__main__':
    with open('url_list.csv', mode='r') as file:
        reader = csv.reader(file)
        url_list = [row for row in reader]
    with open('url_list_Fundamentals.csv', mode='r') as file:
        reader = csv.reader(file)
        url_list_f = [row for row in reader]
    with open('url_list_National_Statistics.csv', mode='r') as file:
        reader = csv.reader(file)
        url_list_n = [row for row in reader]
    with open('url_list_Price&V.csv', mode='r') as file:
        reader = csv.reader(file)
        url_list_p = [row for row in reader]
    print('OK')
    file_path = 'table_code.csv'
    info_dic = csv_to_nested_dict(file_path)
    sl_dict.save(info_dic, '../data/info_dict.pkl')

