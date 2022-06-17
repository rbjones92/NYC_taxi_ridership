# Robert Jones
# 4/30/2022
# Scraping Wunderground for table data 

import datetime
import requests # For DLing HTML
from bs4 import BeautifulSoup as bs # To work with HTML


# Date Range 2009-01-01 to 2022-01-01
# f = open("C:\\Users\\Bob\\Desktop\\SpringBoard\\Python_Projects\\NYC_Taxi_Capstone\\Project_Docs\\date_range.txt", "r")

def get_dates():
    # Find Range of Dates
    d1 = datetime.date(2009,1,1)
    d2 = datetime.date(2022,1,1)

    dd = [d1 + datetime.timedelta(days=x) for x in range((d2-d1).days + 1)]
    date_list = []

    for d in dd:
        date_list.append(str(d))
    return date_list

dates = get_dates()


def get_URLS():
    url_list = []
    domain = 'https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA/date/'
    for i in range(0,len(dates),1):
        url_list.append(str(domain + dates[i]))
    return url_list

url_list = get_URLS()

# Parse into text
def get_soup():
    # Loop thru all URLs
    '''
    for i in range(0,len(url_list),1):
        print(bs(requests.get(url_list[i]).text, 'html.parser'))
        # return bs(requests.get(url_list[i]).text, 'html.parser')
    '''
    # Test on one
    soup = bs(requests.get(url_list[0]).text, 'html.parser')
    return soup
    

def get_data():
    data = []
    table = get_soup().find('table')
    table_body = get_soup().find('tbody')
    print(table_body)


    # table_body = table.find('tbody')
    # rows = table_body.find_all('tr')
    print(table)
    '''
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele]) 
    '''
    print(data)
    
get_data()

'''
for link in get_soup(URL).find_all('a'):
    csv_link = link.get('href')
    if FILETYPE in csv_link:
        print(csv_link)
        print(link.text)
        
        with open(link.text, 'wb') as file:
            response = requests.get(csv_link)
            file.write(response.content)
'''
