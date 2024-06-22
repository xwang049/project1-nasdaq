import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import time
import csv

def fetch_page_urls(page, driver):
    url = f'https://data.nasdaq.com/search?page={page + 1}'
    #url = f'https://data.nasdaq.com/search?filters=%5B%22Prices%20%26%20Volumes%22%5D&page={page + 1}'
    #url = f'https://data.nasdaq.com/search?filters=%5B%22Fundamentals%22%5D&page={page + 1}'
    #url = f'https://data.nasdaq.com/search?filters=%5B%22National%20Statistics%22%5D&page={page + 1}'
    driver.get(url)
    time.sleep(10)  # Adjust the sleep time as needed
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, "html.parser")
    #driver.quit()
    product_cards = soup.findAll("a", attrs={"class": "product-card__overview-content"})
    return [card.get('href') for card in product_cards]

max_page = 15

def scrape_and_save_urls():
    url_list = []
    driver = uc.Chrome(headless=False, use_subprocess=False)
    for page in range(max_page):
        urls = fetch_page_urls(page, driver)
        url_list.extend(urls)
    driver.quit()

    filename = "url_list.csv"
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        for item in url_list:
            item1 = f'https://data.nasdaq.com' + item
            writer.writerow([item1])
    
    print(f"Data has been written to {filename}")

if __name__ == '__main__':
    scrape_and_save_urls()