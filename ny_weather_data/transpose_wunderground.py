# Robert Jones
# Transpose parquet files from scrape_wunderground.py
# 6.22.22

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType



os.chdir('C:\\Users\\Robert.Jones\\Desktop\\Springboard\\Capstone\\wunderground\\')
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


spark = SparkSession.builder.getOrCreate()

spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")


weather_schema = StructType([ \
    StructField("date",StringType(),True), \
    StructField("time",StringType(),True), \
    StructField("temp",StringType(),True), \
    StructField("dew", StringType(), True), \
    StructField("humid", StringType(), True), \
    StructField("wind_direction", StringType(), True), \
    StructField("wind_speed",StringType(),True), \
    StructField("wind_gust",StringType(),True), \
    StructField("pressure",StringType(),True), \
    StructField("precip",StringType(),True), \
    StructField("condition",StringType(),True), 
])


folder = 'C:\\Users\\Robert.Jones\\Desktop\\Springboard\\Capstone\\wunderground\\parquet_files'

for filename in os.listdir(folder):
    f = os.path.join(folder,filename)
    if os.path.isfile(f):
        df = spark.read.format('parquet').load(f)
        data_list = []


        for i in range(0,int(df.count()/10),1):
            time = df.collect()[0:df.count():10][i][1]
            temp = df.collect()[1:df.count():10][i][1]
            dew = df.collect()[2:df.count():10][i][1]
            humid = df.collect()[3:df.count():10][i][1]
            wind_direction = df.collect()[4:df.count():10][i][1]
            wind_speed = df.collect()[5:df.count():10][i][1]
            wind_gust = df.collect()[6:df.count():10][i][1]
            pressure = df.collect()[7:df.count():10][i][1]
            precip = df.collect()[8:df.count():10][i][1]
            condition = df.collect()[9:df.count():10][i][1]
            date = df.columns[1]
            
            vals = [date,time,temp,dew,humid,wind_direction,wind_speed,wind_gust,pressure,precip,condition]
            data_list.append(vals)
        

        
        df2 = spark.createDataFrame(data= data_list,schema=weather_schema)
        df2.write.parquet(f'C:\\Users\\Robert.Jones\\Desktop\\Springboard\\Capstone\\wunderground\\transposed_parquet_2\\transposed_wunderground_{date}.parquet')


