# Robert Jones
# 2.6.23
# Get all distinct schemas from yellow taxi data
#...write to file
#...using 'inferSchema','true'

COUNTER = 0

import pandas as pd
import os
import sys
from pyspark.sql import SparkSession
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
spark = SparkSession.builder.getOrCreate()

schema_df = pd.DataFrame(columns=['File','Schema'])
directory = 'nyc_taxi_ridership/yellow/' 

all_data = []

for file in os.listdir(directory):
     all_data.append(file)

def get_schema(data,counter):

    df = spark.read.option('inferSchema','true').format('parquet').load(directory+data)
    schema_df.at[counter,'File'] = data
    schema_df.at[counter,'Schema'] = df.dtypes

for data in all_data:
    get_schema(data,COUNTER)
    COUNTER = COUNTER + 1

schema_df.to_csv('nyc_taxi_ridership/yellow_schema/yellow_schema_df.csv')