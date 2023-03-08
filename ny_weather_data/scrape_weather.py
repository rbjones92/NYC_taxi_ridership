# Robert Jones 
# 3.8.22
# Scraping wunderground website for weather data in New York from 2009 to 2023. 
# ...Update old version ('selenium_scrape_daily_wunderground.py')

import datetime
from bs4 import BeautifulSoup as BS
from selenium import webdriver
import pandas as pd
import time


# Function to find ranges of dates
def get_dates():
    # Test Range
    # d1 = datetime.date(2018,11,17)
    # d2 = datetime.date(2018,11,18)

    # Find Range of Dates
    d1 = datetime.date(2018,1,1)
    d2 = datetime.date(2018,1,3)
    dd = [d1 + datetime.timedelta(days=x) for x in range((d2-d1).days + 1)]
    date_list = []
    for d in dd:
        date_list.append(str(d))
    return date_list

# function to load wunderground data (without this it has no records to show)
def render_page(url):
    executable_path = 'C:/Users/Robert.Jones/OneDrive - Central Coast Energy Services, Inc/Documents/chromedriver/chromedriver.exe' # Enter Executable path
    driver = webdriver.Chrome(executable_path=executable_path)
    driver.get(url)
    time.sleep(3)
    r = driver.page_source
    driver.quit()
    return r

def list_transpose(data_list):
    res_list = [[item.replace('%', '') for item in lst] for lst in data_list]
    res_list = [[item.replace(u'\xa0', u'') for item in lst] for lst in res_list]
    res_list = [[item.replace('°F','') for item in lst] for lst in res_list]
    res_list = [[item.replace('°in','') for item in lst] for lst in res_list]
    res_list = [[item.replace('°%','') for item in lst] for lst in res_list]
    res_list = [[item.replace('°mph','') for item in lst] for lst in res_list]
    final_list = [[item.replace('°','') for item in lst] for lst in res_list]
    return final_list

def set_schema(df):
    # To Interger
    df[["Temperature","Dew_Point", "Humidity","Wind_Speed","Wind_Gust"]] = df[["Temperature","Dew_Point", "Humidity","Wind_Speed","Wind_Gust"]].apply(pd.to_numeric)
    df[['Pressure','Precipitation']] = df[['Pressure','Precipitation']].apply(pd.to_numeric)
    # To DateTime
    df['datetime'] = df['datetime'].apply(pd.to_datetime)
    # To String
    df[['Wind','Condition']] = df[['Wind','Condition']].applymap(str)
    return df


def scraper(page, dates):
    # function to scrape wunderground
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
            cols = ['Time','Temperature','Dew_Point','Humidity','Wind','Wind_Speed','Wind_Gust','Pressure','Precipitation','Condition','Date']
            for i in range(0,len(data),10):
                snip_data = []
                snip_data.append(data[i:i+10])
                # Strip of Weird Characters
                snip_data = list_transpose(snip_data)
                snip_data[0].append(d)
                df = pd.DataFrame(snip_data,columns=cols)
                df['datetime'] = df['Date'] + ' ' + df['Time']
                df = df.drop(['Date','Time'],axis=1)
                # Set Schema
                df = set_schema(df)
                df_daily.append(df)

            df_daily = pd.concat(df_daily)
            path = 'C:/Users/Robert.Jones/OneDrive - Central Coast Energy Services, Inc/Desktop/Springboard/Capstone/wunderground/parquet_files'
                        
            df_daily.to_parquet(f'{path}/NY_Weather{d}.parquet')
            
        except AttributeError:
            continue

dates = get_dates()
page = 'https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA/date/'

df = scraper(page, dates)

