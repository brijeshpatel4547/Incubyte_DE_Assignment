# Databricks notebook source
# DBTITLE 1,Connecting ADLS and Databricks
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "1b583470-a522-45f4-b113-c7b764ff0d5d", #Client ID
"fs.azure.account.oauth2.client.secret": 'SecretValue', #Value of secret key
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/b5a2d267-4ba4-4f75-a2c3-19ef56c332b5/oauth2/token"} #Tenant ID

dbutils.fs.mount(
source = "abfss://results@incubytestorage.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/results",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/results"

# COMMAND ----------

# DBTITLE 1,Importing Libraries and Session
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder.appName("PartitionData").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Set Up Connection to PGSQL
# Define the connection parameters
jdbcHostname = "incubyte-pgsql.postgres.database.azure.com"
jdbcPort = 5432
jdbcDatabase = "incubyte"
jdbcUsername = "postgres"
jdbcPassword = "pass"

# Create the JDBC URL
jdbcUrl = f"jdbc:postgresql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}"

# Set the connection properties
connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "org.postgresql.Driver"
}

# Read data from PostgreSQL
patients_df = spark.read.jdbc(url=jdbcUrl, table="public.patients", properties=connectionProperties)




# COMMAND ----------

# DBTITLE 1,Transformation


# Import Dat
patients_df = patients_df.drop('H')

    # Converting Strings to Dates for better Processing
patients_df = patients_df.withColumn("customer_open_date", to_date(col("customer_open_date"),"yyyyMMdd")) \
            .withColumn("last_consulted_date", to_date(col("last_consulted_date"),"yyyyMMdd")) \
            .withColumn("dob", to_date(col("dob"),"MMddyyyy"))

    # Adding Additional Derivative Columns
    # Add Age
patients_df = patients_df.withColumn("Age",(datediff(current_date(), col("dob")) / 365.25).cast("int"))

    # days since last consulted >30 
patients_df = patients_df.withColumn("Days_Since_Last_Consulted_>_30", when((datediff(current_date(),"last_consulted_date").cast("int")) >= 30, True).otherwise(False))


# COMMAND ----------

# DBTITLE 1,Viewing Data as Pandas DF
patients_df.toPandas().head()

# COMMAND ----------

# DBTITLE 1,Partitioning Data based on countries
patients_df.write.format("csv").mode("overwrite").partitionBy("country").save("/mnt/results/partitionedfiles")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/results/partitionedfiles"
