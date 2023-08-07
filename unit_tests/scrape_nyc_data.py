# Robert Jones
# 7.18.23
# Design Unit tests for...
# ...Scraping https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
#...for yellow, green, for hire vehicle (FHV), and high volume for hire vehicle (HVFHV) data

import pandas as pd
import requests
import os

class ScrapeNyTaxi:
    yellow_dates = pd.date_range('2009-01-01','2022-12-01',freq='MS').strftime("%Y-%m").to_list()
    
    @staticmethod
    def grab_data(date, directory):
        # Download parquet files
            try:
                url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet'
                response = requests.get(url, allow_redirects=True)
                os.makedirs(directory, exist_ok=True)  # Create the directory if it doesn't exist
                file_path = os.path.join(directory, f'{directory}_{date}.parquet')
                open(file_path, 'wb').write(response.content)
            except Exception as e:
                print(f'Exception: {e}')

    @classmethod
    def get_all_data(cls):    
        for date in cls.yellow_dates:
            directory = 'yellow'
            cls.grab_data(date=date, directory=directory)

                
