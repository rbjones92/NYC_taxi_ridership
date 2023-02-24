# Robert Jones
# Transpose parquet files from scrape_wunderground.py
# ...Columns are arranged as such from web-scraping, see line 4
''' # Fields repeat every 10 lines
{"2011-01-01":"11:51 PM","__index_level_0__":"Time"}
{"2011-01-01":"43 °F","__index_level_0__":"Temperature"}
{"2011-01-01":"29 °F","__index_level_0__":"Dew_Point"}
{"2011-01-01":"58 °%","__index_level_0__":"Humidity"}
{"2011-01-01":"SSW","__index_level_0__":"Wind"}
{"2011-01-01":"6 °mph","__index_level_0__":"Wind_Speed"}
{"2011-01-01":"0 °mph","__index_level_0__":"Wind_Gust"}
{"2011-01-01":"30.11 °in","__index_level_0__":"Pressure"}
{"2011-01-01":"0.0 °in","__index_level_0__":"Precipitation"}
{"2011-01-01":"Fair","__index_level_0__":"Condition"}
{"2011-01-01":"12:51 AM","__index_level_0__":"Time"}
{"2011-01-01":"43 °F","__index_level_0__":"Temperature"}
{"2011-01-01":"29 °F","__index_level_0__":"Dew_Point"}
{"2011-01-01":"58 °%","__index_level_0__":"Humidity"}
{"2011-01-01":"SSW","__index_level_0__":"Wind"}
{"2011-01-01":"7 °mph","__index_level_0__":"Wind_Speed"}
{"2011-01-01":"0 °mph","__index_level_0__":"Wind_Gust"}
{"2011-01-01":"30.11 °in","__index_level_0__":"Pressure"}
{"2011-01-01":"0.0 °in","__index_level_0__":"Precipitation"}
{"2011-01-01":"Fair","__index_level_0__":"Condition"}
{"2011-01-01":"1:51 AM","__index_level_0__":"Time"}
'''
# ...Transpose rows into columns
# 2.22.23

from datetime import datetime
import os
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


spark = SparkSession\
    .builder\
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")\
    .config("spark.driver.memory", "2g")\
    .getOrCreate()


# Time it
start = datetime.now()

weather_schema = StructType([ \
    StructField("date",StringType(),True), \
    StructField("time",StringType(),True), \
    StructField("temp (F)",StringType(),True), \
    StructField("dew (F)", StringType(), True), \
    StructField("humid (%)", StringType(), True), \
    StructField("wind_direction", StringType(), True), \
    StructField("wind_speed (MPH)",StringType(),True), \
    StructField("wind_gust (MPH)",StringType(),True), \
    StructField("pressure (inches)",StringType(),True), \
    StructField("precip (inches)",StringType(),True), \
    StructField("condition",StringType(),True), 
])

folder = 'testing_weather_data/'

# Get all files in directory
for filename in os.listdir(folder):
    f = os.path.join(folder,filename)
    if os.path.isfile(f):
        df = spark.read.format('parquet').load(f)
        count = df.count()
        collect_list = df.collect()
        date = filename[10:20]

        data_list = []
        # Loop thru data, collecting in chunks of 10
        for i in range(0,int(count/10),1):

            time = collect_list[0:count:10][i][1]
            temp = collect_list[1:count:10][i][1]
            dew = collect_list[2:count:10][i][1]
            humid = collect_list[3:count:10][i][1]
            wind_direction = collect_list[4:count:10][i][1]
            wind_speed = collect_list[5:count:10][i][1]
            wind_gust = collect_list[6:count:10][i][1]
            pressure = collect_list[7:count:10][i][1]
            precip = collect_list[8:count:10][i][1]
            condition = collect_list[9:count:10][i][1]

            '''
            time = df.collect()[0:count:10][i][0]
            temp = df.collect()[1:count:10][i][0]
            dew = df.collect()[2:count:10][i][0]
            humid = df.collect()[3:count:10][i][0]
            wind_direction = df.collect()[4:count:10][i][0]
            wind_speed = df.collect()[5:count:10][i][0]
            wind_gust = df.collect()[6:count:10][i][0]
            pressure = df.collect()[7:count:10][i][0]
            precip = df.collect()[8:count:10][i][0]
            condition = df.collect()[9:count:10][i][0]
            date = df.columns[0]
            '''

            vals = [date,time,temp,dew,humid,wind_direction,wind_speed,wind_gust,pressure,precip,condition]
            data_list.append(vals)

        # Replace special characters from list
        res_list = [[item.replace('%', '') for item in lst] for lst in data_list]
        res_list = [[item.replace(u'\xa0', u'') for item in lst] for lst in res_list]
        res_list = [[item.replace('°F','') for item in lst] for lst in res_list]
        res_list = [[item.replace('°in','') for item in lst] for lst in res_list]
        res_list = [[item.replace('°%','') for item in lst] for lst in res_list]
        res_list = [[item.replace('°mph','') for item in lst] for lst in res_list]
        final_list = [[item.replace('°','') for item in lst] for lst in res_list]

        # Create DF with required schema (can adjust DataTypes later)
        df2 = spark.createDataFrame(data=final_list,schema=weather_schema)
        # Write data
        df2.write.parquet(f'testing_transposed_data/transposed_wunderground_{date}.parquet',mode='overwrite')

# End Time it
end = datetime.now()

# Total time
total_time = end - start
print(f'Process took {total_time} seconds')