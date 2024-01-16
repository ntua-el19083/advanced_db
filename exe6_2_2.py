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
    spark = SparkSession.builder.appName("Query_4_2_2")\
    .getOrCreate()
    return spark

def min_distance_area(lat, long, list):
    distance = 1000
    index = 0
    for i in list:
        temp = get_distance(lat,long,i[0],i[1])
        if(temp < distance):
            distance = temp
            index = i[2]
    return index

spark = initialize()
sc = spark.sparkContext

baselocation = "hdfs://okeanos-master:54310/user/user/"

lapd = spark.read.csv(baselocation + "LAPD_Police_Stations.csv",header=True).\
    select(col('Y').cast(FloatType()),col('X').cast(FloatType()),col('DIVISION').cast(StringType()).alias('AREA NAME'))

lapd_list = lapd.collect()

#returns the index of the closest area
def min_distance2(x,y):
    return min_distance_area(x,y,lapd_list)

min_distance_udf = udf(min_distance2,StringType())
get_distance_udf = udf(get_distance,FloatType())

#similar to the previous, instead of the min distance, we add a column with the index of the closest station
per_station_nearest = spark.read.csv([baselocation + "crime-data-2010-to-2019.csv",baselocation + "crime-data-2020-to-present.csv"],header=True)\
    .select(col('LAT').cast(FloatType()),col('LON').cast(FloatType()),\
    col('Weapon Used Cd').cast(IntegerType()).between(100,199).alias('check'))\
        .filter((col('check') == True) & (col('LAT') != 0))\
           .withColumn("AREA NAME",min_distance_udf(col('LAT'),col('LON')))

#we use that column to join with the lapd df and then calculate the distance
per_station_nearest = per_station_nearest.join(lapd,"AREA NAME")\
           .withColumn("distance",get_distance_udf(col('LAT'),col('LON'),col('Y'),col('X')))

#Finally we group by area name, count, find the mean of the distance and sort
count = per_station_nearest.groupBy('AREA NAME').count()
avg = per_station_nearest.groupBy('AREA NAME').mean('distance')
per_station_nearest = avg.join(count,'AREA NAME').orderBy('count',ascending = False)
per_station_nearest.show(21)
