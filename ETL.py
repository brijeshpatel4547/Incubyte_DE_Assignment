import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder.appName("PartitionData").getOrCreate()

def ETL():

    # Import Data
    customer_df = spark.read.option("delimiter","|").option("header", "true").csv(r"C:\Users\panka\OneDrive\Desktop\customer_data.csv").drop('H')

    # Converting Strings to Dates for better Processing
    customer_df = customer_df.withColumn("Open_Date", to_date(col("Open_Date"),"yyyyMMdd")) \
            .withColumn("Last_Consulted_Date", to_date(col("Last_Consulted_Date"),"yyyyMMdd")) \
            .withColumn("DOB", to_date(col("DOB"),"MMddyyyy"))

    # Adding Additional Derivative Columns
    # Add Age
    customer_df = customer_df.withColumn("Age",(datediff(current_date(), col("DOB")) / 365.25).cast("int"))

    # days since last consulted >30 
    customer_df = customer_df.withColumn("Days_Since_Last_Consulted_>_30", when((datediff(current_date(),"Last_Consulted_Date").cast("int")) >= 30, True) \
                                    .otherwise(False))

    # Store the output as different files
    customer_df.write.option("header", True).partitionBy("Country").mode("overwrite").csv(r'C:\Users\panka\OneDrive\Desktop\Results')


ETL()
