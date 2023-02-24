# Robert Jones 
# 6.16.2022
# Scraping wunderground website for weather data in New York from 2009 to 2022. 

import datetime
from bs4 import BeautifulSoup as BS
from selenium import webdriver
import pandas as pd
import time

# Function to find ranges of dates
def get_dates():
    # Find Range of Dates
    d1 = datetime.date(2018,11,17)
    d2 = datetime.date(2022,1,1)

    dd = [d1 + datetime.timedelta(days=x) for x in range((d2-d1).days + 1)]
    date_list = []

    for d in dd:
        date_list.append(str(d))
    return date_list


# function to load wunderground data (without this it has no records to show)
def render_page(url):
    executable_path = '' # Enter Executable path
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(3)
    r = driver.page_source
    driver.quit()
    return r


# function to scrape wunderground
def scraper(page, dates):

    for d in dates:

        url = str(str(page) + str(d))

        r = render_page(url)

        soup = BS(r, "html.parser")
        container = soup.find('lib-city-history-observation')
        check = container.find('tbody')

        data = []
        try:
            for c in check.find_all('tr', class_='ng-star-inserted'):
                for i in c.find_all('td', class_='ng-star-inserted'):
                    trial = i.text
                    trial = trial.strip('  ')
                    data.append(trial)
            
            df_daily = []
            for i in range(0,len(data),10):
                df = pd.DataFrame(data[i:i+10],columns=[d],index=['Time','Temperature','Dew_Point','Humidity','Wind','Wind_Speed','Wind_Gust','Pressure','Precipitation','Condition'])
                df_daily.append(df)

            df_daily = pd.concat(df_daily)
            df_daily.to_parquet(f'NY_Weather{d}.parquet')
            
        except AttributeError:
            continue


dates = get_dates()
page = 'https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA/date/'

df = scraper(page, dates)


