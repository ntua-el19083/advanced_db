from pyspark.sql import SparkSession

def initialize(number_of_executors):
    spark = SparkSession.builder.appName("Query_2_rdd")\
    .getOrCreate()
    return spark

def part_of_day(x):
    x  = int(x)
    if x >=500 and x < 1200:
        return('morning'),1
    elif x >= 1200 and x<1700:
        return('afternoon'),1
    elif x>=1700 and x < 2100:
        return('evening'),1
    else:
        return('night'),1

spark = initialize(4)
sc = spark.sparkContext

baselocation = "hdfs://okeanos-master:54310/user/user/"

#read the file, split it by commas, there are some commas in the fields, so we can't check the 16th column were the
#Premis disc is, so we check all of the fields for the string STREET. When we don't find the string we map the value
#None and then filter those entries out. We use part_of_day that returns (part_of_day,1), then reduce by the keys.

time_location = sc.textFile(baselocation + "crime-data-2010-to-2019.csv")\
    .map(lambda x: x.split(','))\
    .map(lambda x: x[3] if ('STREET' in x) else None)\
    .filter(lambda x:x != None)\
    .map(part_of_day)\
    .reduceByKey(lambda x,y: x+y)\

#Do the same for the second file.
time_location2 = sc.textFile(baselocation + "crime-data-2020-to-present.csv")\
    .map(lambda x: x.split(','))\
    .map(lambda x: x[3] if 'STREET' in x else None)\
    .filter(lambda x:x != None)\
    .map(part_of_day)\
    .reduceByKey(lambda x,y: x+y)\

#Add the two RDDS, reduce again by key and then sort the results and collect.
time_location = time_location.union(time_location2)\
    .reduceByKey(lambda x,y: x+y)\
    .sortBy(lambda x: x[1], ascending=False).collect()

print(time_location)
