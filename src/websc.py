import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import time
import csv
import pandas as pd
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

def table_to_df(table):
    try:
        header = [th.text for th in table.find('thead').find_all('th')]
        rows = []
        for tr in table.find('tbody').find_all('tr'):
            cells = []
            for td in tr.find_all('td'):
                a = td.find('a')
                if a:
                    cell_content = a.text
                else:
                    cell_content = td.text
                cells.append(cell_content)
            rows.append(cells)
        df = pd.DataFrame(rows, columns=header)
        
    except:
        table_code = None
        df = pd.DataFrame({'TABLE CODE': [table_code]})
    return df


def find_code(page):
    driver = uc.Chrome(headless=False, use_subprocess=False)
    driver.get(page[0])
    time.sleep(6)
    print('Success!' + page[0])
    page_source_test = driver.page_source
    soup_test = BeautifulSoup(page_source_test, "html.parser")
    try:
            table = soup_test.find("section", attrs={"data-anchor": "anchor-data-organization"}).find("div", attrs={"class": "documentation-markdown"}).find("table")
    except:
            table = None
    df = table_to_df(table)
    df['urls'] = page[0]
    driver.quit()
    return df
     

def get_table_code(url_list):
    dataframes = []
    for page in url_list:
        df = find_code(page)
        dataframes.append(df)
    return dataframes

if __name__ == '__main__':
    with open('url_list.csv', mode='r') as file:
        reader = csv.reader(file)
        url_list = [row for row in reader]
    tc = []
    dataframes = get_table_code(url_list)
    for i in dataframes:
        try: 
            a = i['TABLE CODE'].values.tolist()
            tc.append(i['TABLE CODE'].values.tolist())
        except:
            try:
                a = i['Quandl Code'].values.tolist()
                tc.append(i['TABLE CODE'].values.tolist())
            except:
                tc.append([None])

    with open('table_code.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(tc)
    print(f'Data saved to table_code.csv')
