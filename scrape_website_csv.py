# Robert Jones
# 2/1/2022
# Capstone Project w/ Springboard
# Scraping a website to download csv 

import requests # For DLing HTML
from bs4 import BeautifulSoup as bs # To work with HTML

DOMAIN = 'https://www1.nyc.gov'
URL = 'https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
FILETYPE = '.csv'


def get_soup(url):
    return bs(requests.get(url).text, 'html.parser')


for link in get_soup(URL).find_all('a'):
    csv_link = link.get('href')
    if FILETYPE in csv_link:
        print(csv_link)
        print(link.text)
        '''
        with open(link.text, 'wb') as file:
            response = requests.get(csv_link)
            file.write(response.content)
            '''
            