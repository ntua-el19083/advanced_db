from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StringType,StructType,FloatType,StructField
from pyspark.sql.functions import col, udf

def initialize():
    spark = SparkSession.builder.appName("Query_2_df")\
    .getOrCreate()
    return spark

def part_of_day(x):
    x  = int(x)
    if x >=500 and x < 1200:
        return('morning')
    elif x >= 1200 and x<1700:
        return('afternoon')
    elif x>=1700 and x < 2100:
        return('evening')
    else:
        return('night')

spark = initialize()
sc = spark.sparkContext

baselocation = "hdfs://okeanos-master:54310/user/user/"

#load the two files, select the time and Premis Desc, filter so Premis==STREET. Add the column part of day
#and then group by the value, count each value entries and sort them.
part_of_day_udf = udf(part_of_day,StringType())
df = spark.read.csv([baselocation + "crime-data-2010-to-2019.csv",baselocation + "crime-data-2020-to-present.csv"],header=True)\
    .select('TIME OCC','Premis Desc')\
    .filter( col("Premis Desc")== 'STREET')\
    .withColumn("part of day", part_of_day_udf(col("TIME OCC")))\
    .groupBy("part of day").count().orderBy("count",ascending = False).show()                                                                            
