# Robert Jones
# Transpose parquet files from scrape_weather.py

from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType, DoubleType
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession\
    .builder\
    .config("spark.driver.memory", "8g")\
    .getOrCreate()

# Time it
start = datetime.now()


weather_schema = StructType([\
    StructField("temp (F)",IntegerType(),True), \
    StructField("dew (F)", IntegerType(), True), \
    StructField("humid (%)", IntegerType(), True), \
    StructField("wind_direction", StringType(), True), \
    StructField("wind_speed (MPH)",IntegerType(),True), \
    StructField("wind_gust (MPH)",IntegerType(),True), \
    StructField("pressure (inches)",DoubleType(),True), \
    StructField("precip (inches)",DoubleType(),True), \
    StructField("condition",StringType(),True),\
    StructField("datetime",DateType(),True),\
    StructField("__index_level_0__",StringType(),True),
])

folder = 'C:/Users/Robert.Jones/OneDrive - Central Coast Energy Services, Inc/Desktop/Springboard/Capstone/wunderground/parquet_files'

# df = spark.read.schema(weather_schema).parquet(f'{folder}/*.parquet')
df = spark.read.option('inferSchema','true').parquet(f'{folder}/*.parquet')
df = df.drop('__index_level_0__')

# Write data
df.write.parquet(f'testing_transposed_data')

# End Time it
end = datetime.now()
# Total time
total_time = end - start
print(f'Process took {total_time} seconds')