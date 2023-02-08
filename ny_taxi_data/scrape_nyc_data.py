# Robert Jones
# 2.6.23
# Scraping https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
#...for yellow, green, for hire vehicle (FHV), and high volume for hire vehicle (HVFHV) data

# URL Range January 2009 to January 2022
# ...https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-01.parquet
# ...https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet

import pandas as pd
import requests

dates = pd.date_range('2009-01-01','2022-12-01',freq='MS').strftime("%Y-%m").to_list()

# yellow_dates
# green_dates
# for_hire_dates
# high_volume_dates

for date in reversed(dates):
    
    try:
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet'
        response = requests.get(url, allow_redirects=True)
        open(f'yellow/yellow_{date}.parquet','wb').write(response.content)

    except Exception as e:
        print(f'Exception: {e}')
        continue

