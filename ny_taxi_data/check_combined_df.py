import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.memory", "15g").getOrCreate()

file = 'ny_taxi_data/combined_df/combined_df_2.8.23.parquet'

df = spark.read.parquet(file)

df.printSchema()
print(df.count())