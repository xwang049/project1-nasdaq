import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import time
import csv
import pandas as pd

def table_to_df(tables):
    header = [th.text for th in tables[0].find('thead').find_all('th')]
    # Extract the table rows
    rows = []
    for tr in tables[0].find('tbody').find_all('tr'):
        cells = []
        for td in tr.find_all('td'):
            # If the cell contains a link, extract the text
            a = td.find('a')
            if a:
                cell_content = a.text
            else:
                cell_content = td.text
            cells.append(cell_content)
        rows.append(cells)

    # Create the DataFrame
    df = pd.DataFrame(rows, columns=header)

    # Display the DataFrame
    return df


if __name__ == '__main__':
    driver = uc.Chrome(headless=True,use_subprocess=False)
    url_list = []
    for page in range(15):
        driver.get(f'https://data.nasdaq.com/search?page={page + 1}')
        time.sleep(15)
        #driver.save_screenshot('nowsecure.png')
        page_sourse = driver.page_source
        soup = BeautifulSoup(page_sourse, "html.parser")
        product_cards = soup.findAll("a", attrs={"class": "product-card__overview-content"})
        href_list = [card.get('href') for card in product_cards]
        url_list.extend(href_list)

    filename = "url_list.csv"
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        for item in url_list:
            item1 = f'https://data.nasdaq.com' + item
            writer.writerow([item])
    print(f"Data has been written to {filename}")