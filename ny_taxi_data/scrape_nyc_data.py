# Robert Jones
# 2.6.23
# Scraping https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
#...for yellow, green, for hire vehicle (FHV), and high volume for hire vehicle (HVFHV) data

# Example URLs
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2022-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet

import pandas as pd
import requests

yellow_dates = pd.date_range('2009-01-01','2022-12-01',freq='MS').strftime("%Y-%m").to_list()
green_dates = pd.date_range('2013-08-01','2022-12-01',freq='MS').strftime("%Y-%m").to_list()
fh_dates = pd.date_range('2015-01-01','2022-12-01',freq='MS').strftime("%Y-%m").to_list()
hv_dates = pd.date_range('2019-02-01','2022-12-01',freq='MS').strftime("%Y-%m").to_list()

class ScrapeNyTaxi:

    def grab_yellow():
        '''
        Download yellow cab parquet files 
        '''
        for date in yellow_dates:
            
            try:
                url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet'
                response = requests.get(url, allow_redirects=True)
                open(f'yellow/yellow_{date}.parquet','wb').write(response.content)

            except Exception as e:
                print(f'Exception: {e}')
                continue

    def grab_green():
        '''
        Download yellow cab parquet files 
        '''
        for date in green_dates:
            try:
                url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{date}.parquet'
                response = requests.get(url, allow_redirects=True)
                open(f'yellow/yellow_{date}.parquet','wb').write(response.content)

            except Exception as e:
                print(f'Exception: {e}')
                continue

    def grab_fh():
        '''
        Download for hire vehicle parquet files 
        '''
        for date in fh_dates:
            try:
                url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{date}.parquet'
                response = requests.get(url, allow_redirects=True)
                open(f'yellow/yellow_{date}.parquet','wb').write(response.content)

            except Exception as e:
                print(f'Exception: {e}')
                continue            

    def grab_hvfh():
        '''
        Download high volume for hire parquet files 
        '''
        for date in hv_dates:
            try:
                url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{date}.parquet'
                response = requests.get(url, allow_redirects=True)
                open(f'yellow/yellow_{date}.parquet','wb').write(response.content)

            except Exception as e:
                print(f'Exception: {e}')
                continue                      