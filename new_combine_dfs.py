# Robert Jones
# 2.7.23
# Combine yellow DFs - 2011 thru 2022
# ...2009 and 2010 taken out due to different data structure

# Work PC
# directory = 'C:/Users/Robert.Jones/OneDrive - Central Coast Energy Services, Inc/Desktop/Springboard/Capstone/data_pipeline/nyc_data/trip_data/'
# Home PC
directory = 'C:/Users/Robert.Jones/OneDrive - Central Coast Energy Services, Inc/Desktop/Springboard/Capstone/data_pipeline/nyc_data/trip_data'

import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType, IntegerType, DecimalType, BinaryType, ShortType
from pyspark.sql.functions import lit

yellow_schema = StructType([
    StructField('VendorID', LongType(), True),
    StructField('pickup_datetime', TimestampType(), True),
    StructField('dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', DoubleType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('RatecodeID', LongType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('PULocationID', LongType(), True),
    StructField('DOLocationID', LongType(), True),
    StructField('payment_type', LongType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True),
    StructField('airport_fee', IntegerType(), True),
    StructField('taxi_type', StringType(), True),
    ])

green_schema = StructType([
    StructField('VendorID', LongType(), True),
    StructField('lpep_pickup_datetime', TimestampType(), True),
    StructField('lpep_dropoff_datetime', TimestampType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('ehail_fee', LongType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('payment_type', LongType(), True),
    StructField('trip_type', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True),
    StructField('taxi_type', StringType(), True),
    ])

fhv_schema = StructType([
    StructField('dispatching_base_num', StringType(), True),
    StructField('pickup_datetime', TimestampType(), True),
    StructField('dropOff_datetime', TimestampType(), True),
    StructField('PULocationID', DoubleType(), True),
    StructField('DOLocationID', DoubleType(), True),
    StructField('SR_Flag', DoubleType(), True),
    StructField('Affiliated_base_number', StringType(), True),
    StructField('taxi_type', StringType(), True),
    ])

hv_schema_1 = StructType([
    StructField('hvfhs_license_num', StringType(), True),
    StructField('dispatching_base_num', StringType(), True),
    StructField('originating_base_num', StringType(), True),
    StructField('request_datetime', TimestampType(), True),
    StructField('on_scene_datetime', TimestampType(), True),
    StructField('pickup_datetime', TimestampType(), True),
    StructField('dropoff_datetime', TimestampType(), True),
    StructField('PULocationID', LongType(), True),
    StructField('DOLocationID', LongType(), True),
    StructField('trip_miles', DoubleType(), True),
    StructField('trip_time', LongType(), True),
    StructField('base_passenger_fare', DoubleType(), True),
    StructField('tolls', DoubleType(), True),
    StructField('bcf', DoubleType(), True),
    StructField('sales_tax', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True),
    StructField('airport_fee', IntegerType(), True),
    StructField('tips', DoubleType(), True),
    StructField('driver_pay', DoubleType(), True),
    StructField('shared_request_flag', StringType(), True),
    StructField('shared_match_flag', StringType(), True),
    StructField('access_a_ride_flag', StringType(), True),
    StructField('wav_request_flag', StringType(), True),
    StructField('wav_match_flag', StringType(), True),
    StructField('taxi_type', StringType(), True),
    ])

hv_schema_2 = StructType([
    StructField('hvfhs_license_num', StringType(), True),
    StructField('dispatching_base_num', StringType(), True),
    StructField('originating_base_num', StringType(), True),
    StructField('request_datetime', TimestampType(), True),
    StructField('on_scene_datetime', TimestampType(), True),
    StructField('pickup_datetime', TimestampType(), True),
    StructField('dropoff_datetime', TimestampType(), True),
    StructField('PULocationID', LongType(), True),
    StructField('DOLocationID', LongType(), True),
    StructField('trip_miles', DoubleType(), True),
    StructField('trip_time', LongType(), True),
    StructField('base_passenger_fare', DoubleType(), True),
    StructField('tolls', DoubleType(), True),
    StructField('bcf', DoubleType(), True),
    StructField('sales_tax', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True),
    StructField('airport_fee', DoubleType(), True),
    StructField('tips', DoubleType(), True),
    StructField('driver_pay', DoubleType(), True),
    StructField('shared_request_flag', StringType(), True),
    StructField('shared_match_flag', StringType(), True),
    StructField('access_a_ride_flag', StringType(), True),
    StructField('wav_request_flag', StringType(), True),
    StructField('wav_match_flag', StringType(), True),
    StructField('taxi_type', StringType(), True),
    ])

class Taxi_DFs:

    def __init__(self):
        self.spark = SparkSession.builder.config("spark.driver.memory", "8g")\
            .config("spark.sql.files.ignoreCorruptFiles", "true")\
            .getOrCreate()

        
    def make_yellow(self):

        emptyRDD = self.spark.sparkContext.emptyRDD()
        yellow_df = self.spark.createDataFrame(emptyRDD,schema=yellow_schema)

        yellow_list = []

        for file in os.listdir(directory):
            if file.startswith('Yellow'):
                yellow_list.append(file)    

        for file in yellow_list:
            df_yellow = self.spark.read.option('inferSchema','true').parquet(f'{directory}{file}')
            df_yellow = df_yellow.withColumn('taxi_type',lit('yellow'))
            df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime','pickup_datetime')\
                .withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')

            df_yellow.createOrReplaceTempView('Cast')

            df_yellow = self.spark.sql("SELECT BIGINT(VendorID),TIMESTAMP(pickup_datetime),\
                TIMESTAMP(dropoff_datetime),DOUBLE(passenger_count),DOUBLE(trip_distance),\
                BIGINT(RatecodeID),STRING(store_and_fwd_flag),BIGINT(PULocationID),BIGINT(DOLocationID),\
                BIGINT(payment_type),DOUBLE(fare_amount),DOUBLE(extra),DOUBLE(mta_tax),DOUBLE(tip_amount),\
                DOUBLE(tolls_amount),DOUBLE(improvement_surcharge),DOUBLE(total_amount),DOUBLE(congestion_surcharge),\
                DOUBLE(airport_fee),STRING(taxi_type) from Cast")

            yellow_df = df_yellow.union(yellow_df)
            print(f'{file} analyzed')

        yellow_df.printSchema()

        return yellow_df
    
    def make_green(self):

        emptyRDD = self.spark.sparkContext.emptyRDD()
        green_df = self.spark.createDataFrame(emptyRDD,schema=green_schema)

        green_list = []

        for file in os.listdir(directory):
            if file.startswith('Green'):
                green_list.append(file)    

        for file in green_list:    

            df_green = self.spark.read.option('inferSchema','true').parquet(f'{directory}{file}')
            df_green = df_green.withColumnRenamed('lpep_pickup_datetime','pickup_datetime')\
                .withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')
            df_green = df_green.withColumn('taxi_type',lit('green'))

            df_green.createOrReplaceTempView('Cast')

            df_green = self.spark.sql("SELECT BIGINT(VendorID),TIMESTAMP(pickup_datetime),\
                TIMESTAMP(dropoff_datetime),STRING(store_and_fwd_flag),DOUBLE(trip_distance),\
                DOUBLE(fare_amount),DOUBLE(extra),DOUBLE(mta_tax),DOUBLE(tip_amount),\
                DOUBLE(tolls_amount),BIGINT(ehail_fee),DOUBLE(improvement_surcharge),DOUBLE(total_amount),\
                BIGINT(payment_type),DOUBLE(trip_type),DOUBLE(congestion_surcharge),STRING(taxi_type) from Cast")

            green_df = df_green.union(green_df)
            print(f'{file} analyzed')        
        
        green_df.printSchema()

        return green_df

    def make_fhv(self):

        fhv_df = self.spark.read.option('inferSchema','True').parquet(f'{directory}/For*.parquet')
        return fhv_df

    def make_hv(self):

        
        hv_df_1 = self.spark.read.schema(hv_schema_1).parquet(f'{directory}/High*.parquet')
        hv_df_1 = hv_df_1.withColumn('airport_fee',hv_df_1.airport_fee.cast('double'))

        hv_df_2 = self.spark.read.schema(hv_schema_2).parquet(f'{directory}/High*.parquet')

        hv_df = hv_df_1.union(hv_df_2)

        hv_df.show()

        hv_df.write.parquet('combined_hv')
        return hv_df

    def write_dfs(self):

        '''

        hv_df = Taxi_DFs.make_hv(self)
        hv_df.write.parquet('combined_hv')

        # fhv_df = Taxi_DFs.make_fhv(self)
        # fhv_df.write.parquet('combined_fhv')

        yellow_df = Taxi_DFs.make_yellow()
        green_df = Taxi_DFs.make_green()
        fhv_df = Taxi_DFs.make_fhv()
        hv_df = Taxi_DFs.make_hv()

        hv_df.write.parquet('combined_hv.parquet')
        fhv_df.write.parquet('combined_fh.parquet')
        green_df.write.parquet('combined_green.parquet')
        yellow_df.write.parquet('combined_yellow.parquet')
        '''
        
    def combine_all_dfs(self):

        df_yellow = Taxi_DFs.make_yellow()
        df_green = Taxi_DFs.make_green()
        df_fhv = Taxi_DFs.make_fhv()
        df_hv = Taxi_DFs.make_hv()

        df = df_yellow.unionByName(df_green,allowMissingColumns=True)
        print('union yellow and green')
        df = df.unionByName(df_fhv,allowMissingColumns=True)
        print('added for hire')
        df = df.unionByName(df_hv,allowMissingColumns=True)
        print('added high volume')

        # Combine and write
        df.write.parquet('combined_df.parquet')

testing = Taxi_DFs()
testing.make_hv()
