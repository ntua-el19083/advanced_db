from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StringType,StructType,FloatType,StructField
from pyspark.sql.functions import col, udf
from math import sin, cos, sqrt, atan2, radians

def get_distance( lat1, long1 ,lat2 ,long2 ) :
    R = 6373.0
    lat1 = radians(lat1)
    long1 = radians(long1)
    lat2 = radians(lat2)
    long2 = radians(long2)

    dlong = long2 - long1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlong / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance

def initialize():
    spark = SparkSession.builder.appName("Query_4_1_2")\
    .getOrCreate()
    return spark

spark = initialize()
sc = spark.sparkContext

baselocation = "hdfs://okeanos-master:54310/user/user/"

lapd = spark.read.csv(baselocation + "LAPD_Police_Stations.csv",header=True)\
   .select(col('Y').cast(FloatType()),col('X').cast(FloatType()),col('PREC').cast(IntegerType()).alias('AREA '))

get_distance_udf = udf(get_distance,FloatType())

#similar to the previous one, now we group by station and then count, find mean and sort
per_station_recorded = spark.read.csv([baselocation + "crime-data-2010-to-2019.csv",baselocation + "crime-data-2020-to-present.csv"],header=True)\
    .select(col('AREA NAME'), col('AREA ').cast(IntegerType()),col('LAT').cast(FloatType()),col('LON').cast(FloatType()),\
    col('Weapon Used Cd').cast(IntegerType()).between(100,199).alias('check'))\
        .filter((col('check') == True) & (col('LAT') != 0)).join(lapd, 'AREA ')\
           .withColumn("distance",get_distance_udf(col('LAT'),col('LON'),col('Y'),col('X')))

count = per_station_recorded.groupBy('AREA NAME').count()
avg = per_station_recorded.groupBy('AREA NAME').mean('distance')
result = avg.join(count,'AREA NAME').orderBy('count',ascending = False)
result.show(21)
