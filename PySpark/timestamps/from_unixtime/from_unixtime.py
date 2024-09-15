# Databricks notebook source
# MAGIC %md
# MAGIC **from_unixtime**
# MAGIC
# MAGIC - Converts **Unix Time Seconds** to **Date and Timestamp**.
# MAGIC - is used to convert the number of **seconds** from Unix epoch (1970-01-01 00:00:00 UTC) to a **string** representation of the **timestamp**.
# MAGIC
# MAGIC - Converting **Unix Time** to a **Human-Readable Format** of timestamp.
# MAGIC
# MAGIC
# MAGIC       +----------+-------------------+
# MAGIC       |unix_time |timestamp          |
# MAGIC       +----------+-------------------+
# MAGIC       |1648974310|2023-04-03 09:45:10|
# MAGIC       +----------+-------------------+
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      from_unixtime(timestamp: ColumnOrName, format: str = 'yyyy-MM-dd HH:mm:ss') 
# MAGIC
# MAGIC **timestamp:** column of **unix time** values.
# MAGIC
# MAGIC **format:** format to use to convert to (**default:** yyyy-MM-dd HH:mm:ss)
# MAGIC
# MAGIC      from_unixtime(col("timestamp_1")).alias("timestamp_1"),
# MAGIC      from_unixtime(col("timestamp_2"),"MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
# MAGIC      from_unixtime(col("timestamp_3"),"MM-dd-yyyy").alias("timestamp_3")
# MAGIC
# MAGIC **Returns:** string

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import col, exp, current_timestamp, to_timestamp, from_unixtime
from pyspark.sql.functions import *
from pyspark.sql.types import LongType

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/from_unixtime-1.csv", header=True, inferSchema=True)
display(df.limit(10))

# COMMAND ----------

# format columns according to datatypes of Kafka Schema
df = df.withColumn('Input_Timestamp_UTC', f.col('Input_Timestamp_UTC').cast(LongType()))\
       .withColumn('Update_Timestamp_UTC', f.col('Update_Timestamp_UTC').cast(LongType()))

# COMMAND ----------

# current_user() is not directly available as a PySpark function. To resolve this issue, you can use the SQL expression functionality provided by PySpark to execute SQL functions that are not directly exposed in the PySpark API.
df = df.select(current_timestamp().alias("created_timestamp"),
               expr("current_user()").alias("created_by"),
               to_timestamp(from_unixtime(col("Input_Timestamp_UTC")),'yyyy-MM-dd HH:mm:ss').alias('input_timestamp_utc'),
               to_timestamp(from_unixtime(col("Update_Timestamp_UTC")),'yyyy-MM-dd HH:mm:ss').alias('last_update_timestamp_utc'))
display(df.limit(10))
