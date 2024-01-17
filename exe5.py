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

#Function needed to convert Income data from String to Integer
def convert_dollars_to_int(income):
    income=income[1:]
    income_parts = income.split(",")
    cleaned_income="".join(income_parts)

    return int(cleaned_income)

convert_dollars_to_int_udf=udf(convert_dollars_to_int,StringType())

#Function needed to display better names for Descents
def convert_descent(descent):
    descent_dictionary= [
            (descent=='W',"White"),
            (descent=='B',"Black"),
            (descent=='H',"Hispanic"),
            (descent=='C',"Chinese"),
            (descent=='O',"Other"),
            (descent=='A',"Other Asian"),
            (descent=='X',"Unknown"),
    ]

    for condition,result in descent_dictionary:
        if condition:
            return result

    return "Error"

convert_descent_udf=udf(convert_descent,StringType())

#Import CSV file into a dataframe for modificaitons.

#Header variable ensures that the dataframe uses the first line of the CSV file which contians the names of the columns.
#InferSchema variable ensures that the schema of the dataframe is what "it thinks" it is

basic_dataset_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/crime-data-2010-to-2019.csv", header=True, inferSchema=True)

#I need to use both csv files because they are the basic dataset.I make a temporary dataframe and with union i merge them
#I also apply the distinct() function to the result in case there are any duplicates between the two files
temp_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/crime-data-2020-to-present.csv", header=True, inferSchema=True)

basic_dataset_df=basic_dataset_df.union(temp_df)
basic_dataset_df=basic_dataset_df.distinct()

#Prepare the data on LA Income 2015 in a dataframe for calculations
income_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/LA_income_2015.csv", header=True, inferSchema=True)
income_df=income_df.drop("Community")

#Prepare the conversion table of Longitude and Latitude into Zip Codes
geocoding_df=spark.read.csv("hdfs://okeanos-master:54310/user/user/revgecoding.csv",header=True,inferSchema=True)
geocoding_df=geocoding_df.withColumnRenamed("ZIPcode","ZIP Code")

#Keep only needed columns from basic dataset and apply proper date conversion for DATE OCC
results_df=basic_dataset_df.select(
        #Needs to be renamed along with the next one because otherwise its name becomes "function(colName)"
        convert_date_udf(basic_dataset_df["DATE OCC"]).cast(DateType()).alias("DATE OCC"),
        "Vict Descent",
        "LAT",
        "LON",
)

#Only keep crimes of 2015
results_df=results_df.filter(year(results_df["DATE OCC"])==2015)

#Get the ZIP Code of the area a crime is commited in and add it to Dataframe.
#Afterwards drep "LAT" and "LON" Columns because we don't need them
results_df=results_df.join(geocoding_df,on=["LAT","LON"],how="inner")
results_df=results_df.drop("LAT","LON")

#Convert the Income column into integers properly and don't keep any duplicates that may have happend
#afte removing the Community column
income_df=income_df.withColumn("Estimated Median Income",convert_dollars_to_int_udf(income_df["Estimated Median Income"]).cast(IntegerType()))
income_df=income_df.distinct()

#Calculate and keep ZIP Codes of Top 3 and Bottom 3 Income areas in 2 separate dataframes
calculate_top_3_zip_codes_df=income_df.sort(desc("Estimated Median Income")).limit(3)
calculate_bottom_3_zip_codes_df=income_df.sort("Estimated Median Income").limit(3)

#Merge the ZIP Codes of the Top 3 Income Areas with the Bottom 3 and only keep said ZIP Codes in dataframe
requested_zip_df=calculate_top_3_zip_codes_df.union(calculate_bottom_3_zip_codes_df)
requested_zip_df=requested_zip_df.withColumn("ZIP Code",col("Zip Code")).drop("Estimated Median Income")

#For all reported crimes of 2015,only keep the ones that happened in requested Areas with their ZIP Codes.
#Afterwards,only keep Victim Descent because it is the only data needed for the requested result
results_df=results_df.join(requested_zip_df,on="ZIP Code",how="inner").drop("Estimated Median Income","ZIP Code","DATE OCC")

#Discard any crimes where the Victim Descent is Null(For example victimless crimes)
results_df=results_df.filter(results_df["Vict Descent"].isNotNull())

#Count the number of crimes per Victim Descent and sort them from most to least
results_df=results_df.groupBy("Vict Descent").agg((count("Vict Descent")).alias("#"))
results_df=results_df.sort(desc("#"))

#Renaming the Column
results_df=results_df.withColumnRenamed("Vict Descent","Victim Descent")

#Apply function that better describes Descents.In order to modify an existing column,the first
#arguement of withColumn needs to be the same as the name of the column
results_df=results_df.withColumn("Victim Descent",convert_descent_udf(results_df["Victim Descent"]))

#Prints contents of dataframe to make sure it works correctly
results_df.show()

#Prints the number of rows,helpful
print(f"\nNumber of Rows:{results_df.count()}\n")

#Prints schema of dataframe
results_df.printSchema()

spark.stop()