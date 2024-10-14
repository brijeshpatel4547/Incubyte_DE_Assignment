# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Importing Data
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder.appName("PartitionData").getOrCreate()

def ETL():

    # Import Data
    customer_df = spark.read.option("delimiter","|").option("header", "true").csv("dbfs:/FileStore/shared_uploads/brijeshpatel4547@gmail.com/customer_data-1.csv").drop('H')

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
    out_path = "dbfs:/FileStore/shared_uploads/brijeshpatel4547@gmail.com/results"
    customer_df.write.partitionBy("Country").format("csv").mode("overwrite").save(out_path)

ETL()
