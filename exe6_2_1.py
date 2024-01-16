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
    spark = SparkSession.builder.appName("Query_4_2_1")\
    .getOrCreate()
    return spark

def get_year(srt):
    str = srt.split("/")
    str = str[2].split(' ')
    return int(str[0])

def min_distance(lat, long, list):
    distance = 1000
    for i in list:
        temp = get_distance(lat,long,i[0],i[1])
        if(temp < distance):
            distance = temp
    return distance

spark = initialize()
sc = spark.sparkContext

baselocation = "hdfs://okeanos-master:54310/user/user/"

lapd = spark.read.csv(baselocation + "LAPD_Police_Stations.csv",header=True).\
    select(col('Y').cast(FloatType()),col('X').cast(FloatType()),col('PREC').cast(IntegerType()).alias('AREA ')).collect()

#return the distance from the closest police station, we input the lapd dataframe as a list.
def min_distance2(x,y):
    return min_distance(x,y,lapd)

min_distance_udf = udf(min_distance2,FloatType())
get_year_udf = udf(get_year,IntegerType())

#Very similar to the previous query, instead of getting the distance from the assigned station, we find the closest one,
#the join here is not necessary, we use the list in the min_distance_udf.
gun_crimes_nearest = spark.read.csv([baselocation + "crime-data-2010-to-2019.csv",baselocation + "crime-data-2020-to-present.csv"],header=True)\
    .withColumn('year',get_year_udf(col('DATE OCC')))\
    .select(col('LAT').cast(FloatType()),col('LON').cast(FloatType()),\
    col('year'),col('Weapon Used Cd').cast(IntegerType()).between(100,199).alias('check'))\
        .filter((col('check') == True) & (col('LAT') != 0))\
           .withColumn("distance",min_distance_udf(col('LAT'),col('LON')))\
          .groupBy('year').mean('distance').orderBy('avg(distance)').show()
