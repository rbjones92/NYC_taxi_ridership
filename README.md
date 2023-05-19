## Guided Capstone - New York City Taxi Ridership

## Summary
ETL data pipeline scraping both New York City taxi ridership data as well as weather data from wunderground.

## Description
<strong>Taxi Data</strong> - Scrape yellow taxi data from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
<br>
...load to Azure Blob Storage
<br>
<strong>Weather Data</strong> - Scrape wunderground data from https://www.wunderground.com/history/daily/us/ny/new-york-city
<br>
...transform to remove special characters from records and rename columns for units of measurement

## Technologies
- Python 3.10.7
- Azure Databricks
- Azure Blob Storage
- Spark

## End Result DataFrames
Taxi Dataframe

![Alt Text](taxi_data.JPG?raw=true "taxi dataframe")

Weather Dataframe

![Alt Text](weather_data.JPG?raw=true "weather dataframe")
