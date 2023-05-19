# Robert Jones
# 3.8.23
# Transpose parquet files from scrape_weather.py
import os
import sys
from pyspark.sql import SparkSession
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession\
    .builder\
    .config("spark.driver.memory", "8g")\
    .getOrCreate()
# Path to data
folder = 'C:/Users/Robert.Jones/OneDrive - Central Coast Energy Services, Inc/Desktop/Springboard/Capstone/wunderground/parquet_files'
# Read data
df = spark.read.option('inferSchema','true').parquet(f'{folder}/*.parquet')
# Drop unncessary column
df = df.drop('__index_level_0__')
# Write data
df.write.parquet(f'testing_transposed_data')
