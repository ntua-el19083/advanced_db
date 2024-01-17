from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DateType, DoubleType
from pyspark.sql.functions import col, to_date, udf, month, count, year, desc, row_number
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .config("spark.executor.instances", "4") \
    .appName("DF query 1 execution") \
    .getOrCreate()

#Ayto to schema einai boythitiko.Den mporw na to efarmosw sthn spark.scan.csv giati to csv arxeio exei parapanw
#sthlew.Epipleon,aytes xreiazontai epeksergasia

basic_dataset_schema = StructType([
    StructField("Date Rptd", DateType()),
    StructField("DATE OCC", DateType()),
    StructField("Vict Age", IntegerType()),
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType()),
])

#Allazw se agglika.This is a helper function to convert the format of the csv file to the appropriate DateType format that spark uses
#Spark has converters but they didn't work and i more appropriately and easily can do the conversion

def convert_date(date_str):
    parts=date_str.split("/")

    #The last part also contains time.I split with spacebar in order to keep the year,in a way to "Discard" Time
    time_split=parts[2].split(" ")

    year=time_split[0]
    month=parts[0]
    day=parts[1]
    return f"{year}-{month}-{day}"

#Any User fucntion needs to be defined with this command in order to work.I think its name also needs to be different than the original
#but this might not be true
convert_date_udf=udf(convert_date,StringType());

#Import CSV file into a dataframe for modificaitons.

#Header variable ensures that the dataframe uses the first line of the CSV file which contians the names of the columns.
#InferSchema variable ensures that the schema of the dataframe is what "it thinks" it is

basic_dataset_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/crime-data-2010-to-2019.csv", header=True, inferSchema=True)

#I need to use both csv files because they are the basic dataset.I make a temporary dataframe and with union i merge them
#I also apply the distinct() function to the result in case there are any duplicates between the two files
temp_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/crime-data-2020-to-present.csv", header=True, inferSchema=True)

basic_dataset_df=basic_dataset_df.union(temp_df)
basic_dataset_df=basic_dataset_df.distinct()

#For Query 1,i only need the column "Date Rptd" to calculate the result.I select it from the basic dataset
#while also applying DateType cast which will be useful later
basic_dataset_df=basic_dataset_df.select(
        #Needs to be renamed along with the next one because otherwise its name becomes "function(colName)"
        convert_date_udf(basic_dataset_df["Date Rptd"]).cast(DateType()).alias("Date Rptd"),
)

results_df=basic_dataset_df.groupBy(year("Date Rptd").alias("Year"),month("Date Rptd").alias("Month")).agg((count("*")).alias("Total Crimes"))

#Windows allow to "prepare" a modification to the dataframe as well as providing functions that are not
#normally available.In order to assign the position of the month in Top 3,in each year,i need to make an adequate window

#I specify the ordering of the resulting dataframe from the window function row_number() as well as
#the logic of said function,making sure to provide different results for different years with "partitionBy"
window_spec=Window.partitionBy("Year").orderBy("Year",desc("Total Crimes"))

#I apply the window function with the window specifications that are needed
results_df=results_df.withColumn("Top Number",row_number().over(window_spec))

#I keep only the Top 3 Months in terms of total crime occurences,for each year.
results_df=results_df.filter(col("Top Number")<=3)

#Prints contents of dataframe to make sure it works correctly
results_df.show(43)

#Prints the number of rows,helpful
print(f"\nNumber of Rows:{results_df.count()}\n")

#Prints schema of dataframe
results_df.printSchema()

spark.stop()