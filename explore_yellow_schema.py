# Robert Jones
# 2.6.23
# Explore yellow taxi schema
#...and find common columns
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('nyc_taxi_ridership/yellow_schema/yellow_schema_df.parquet')

df = df.groupby('Schema').agg(F.collect_list('File'))

df.coalesce(1).write.json('nyc_taxi_ridership/yellow_schema/yellow_aggregated_schemas.csv')


