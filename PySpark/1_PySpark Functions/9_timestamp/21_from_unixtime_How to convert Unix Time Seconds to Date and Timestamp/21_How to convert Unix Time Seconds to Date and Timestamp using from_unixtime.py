# Databricks notebook source
# MAGIC %md
# MAGIC **from_unixtime**
# MAGIC
# MAGIC - Converts **Unix Time Seconds** to **Date and Timestamp**.
# MAGIC - is used to convert the number of **seconds** from Unix epoch (1970-01-01 00:00:00 UTC) to a **string** representation of the **timestamp**.
# MAGIC - Converting **Unix Time** to a **Human-Readable Format** of timestamp.
# MAGIC
# MAGIC
# MAGIC |unix_time (seconds) |   timestamp          |
# MAGIC |--------------------|----------------------|
# MAGIC |1648974310|2023-04-03 09:45:10|
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - **from_unixtime** expects **timestamp in seconds, not milliseconds**.
# MAGIC - If your **timestamps are in milliseconds, divide by 1000** first:
# MAGIC
# MAGIC       df = df.withColumn("unix_time_sec", (df["unix_time_ms"] / 1000).cast("long"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Syntax
# MAGIC
# MAGIC      from_unixtime(timestamp: ColumnOrName, format: str = 'yyyy-MM-dd HH:mm:ss') 
# MAGIC
# MAGIC **timestamp:** column of **unix time** values.
# MAGIC
# MAGIC **format:**
# MAGIC
# MAGIC       # default: yyyy-MM-dd HH:mm:ss
# MAGIC       from_unixtime(col("timestamp_1")).alias("timestamp_1") 
# MAGIC
# MAGIC       # custom format
# MAGIC       from_unixtime(col("timestamp_2"),"MM-dd-yyyy HH:mm:ss").alias("timestamp_2")
# MAGIC       from_unixtime(col("timestamp_3"),"MM-dd-yyyy").alias("timestamp_3")
# MAGIC
# MAGIC **Returns:** string of **default: yyyy-MM-dd HH:mm:ss**

# COMMAND ----------

# MAGIC %md
# MAGIC **Supported input data types**
# MAGIC - LONG
# MAGIC - BIGINT
# MAGIC - INT
# MAGIC - DOUBLE

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import col, exp, current_timestamp, to_timestamp, from_unixtime
from pyspark.sql.functions import *
from pyspark.sql.types import LongType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Basic Usage
# MAGIC
# MAGIC - Convert **Unix timestamp** to **default timestamp format (yyyy-MM-dd HH:mm:ss)**.

# COMMAND ----------

from pyspark.sql.functions import from_unixtime

data = [(1633072800,), (1622476800,), (1609459200,), (1766476800,), (1998859200,)]
df = spark.createDataFrame(data, ["unix_time"])

df_with_timestamp = df.withColumn("timestamp", from_unixtime("unix_time"))
display(df_with_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Custom Format

# COMMAND ----------

df_with_format = df\
    .withColumn("formatted01", from_unixtime("unix_time", "yyyy/MM/dd HH:mm")) \
    .withColumn("formatted02", from_unixtime("unix_time", "yyyy/MM/dd HH:mm:ss")) \
    .withColumn("formatted03", from_unixtime("unix_time", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("formatted04", from_unixtime("unix_time", "yyyy-MM-dd"))
display(df_with_format)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Convert string time-format (including milliseconds ) to unix_timestamp(double)
# MAGIC - unix_timestamp() function **excludes milliseconds**. 

# COMMAND ----------

df_cust_frmt = spark.createDataFrame([('22-Jul-2018 04:21:18.792 UTC',),
                                      ('23-Jul-2018 04:21:25.888 UTC',),
                                      ('24-Jul-2018 07:24:28.992 UTC',),
                                      ('25-Jul-2019 22:29:55.555 UTC',),
                                      ('26-Jul-2021 12:45:35.666 UTC',)], ['TIME'])

df_cust_frmt = df_cust_frmt\
  .withColumn("date_time", unix_timestamp(col("TIME"),'dd-MMM-yyyy HH:mm:ss.SSS z')) \
  .withColumn("unix_timestamp", unix_timestamp(df_cust_frmt.TIME,'dd-MMM-yyyy HH:mm:ss.SSS z') + substring(df_cust_frmt.TIME,-7,3)/1000)

display(df_cust_frmt)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) set timezone

# COMMAND ----------

from pyspark.sql import functions as F
df = spark.createDataFrame([(1428476400,), (1528598500,)], ['unix_time'])
df.select('*', F.from_unixtime('unix_time').alias("intToString")).display()

# COMMAND ----------

# DBTITLE 1,timeZone: America/Los_Angeles
spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")

# COMMAND ----------

from pyspark.sql import functions as F
df = spark.createDataFrame([(1428476400,), (1528598500,)], ['unix_time'])
df.select('*', F.from_unixtime('unix_time').alias("intToString")).display()

# COMMAND ----------

spark.conf.unset("spark.sql.session.timeZone")

# COMMAND ----------

# DBTITLE 1,timeZone: America/New_York
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

# COMMAND ----------

from pyspark.sql import functions as F
df = spark.createDataFrame([(1428476400,), (1528598500,)], ['unix_time'])
df.select('*', F.from_unixtime('unix_time').alias("intToString")).display()

# COMMAND ----------

spark.conf.unset("spark.sql.session.timeZone")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Basic usage and custom format

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/@azureadb/from_unixtime.csv", header=True, inferSchema=True)
display(df.limit(10))

# COMMAND ----------

# format columns according to datatypes of Kafka Schema
df_cast = df.withColumn('Input_Timestamp_UTC', f.col('Input_Timestamp_UTC').cast(LongType()))\
            .withColumn('Update_Timestamp_UTC', f.col('Update_Timestamp_UTC').cast(LongType()))

display(df_cast.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### from_unixtime:
# MAGIC
# MAGIC      from_unixtime(col("Input_Timestamp_UTC"))
# MAGIC      from_unixtime(col("Update_Timestamp_UTC"))
# MAGIC
# MAGIC **default:** yyyy-MM-dd HH:mm:ss
# MAGIC

# COMMAND ----------

# DBTITLE 1,default and customize format
df_cust = df_cast.select("Input_Timestamp_UTC", "Update_Timestamp_UTC", 
                         from_unixtime(col("Input_Timestamp_UTC")).alias('default_str_input_timestamp_utc'),
                         from_unixtime(col("Update_Timestamp_UTC")).alias('default_str_last_update_timestamp_utc'),
                         from_unixtime(col("Input_Timestamp_UTC"), 'MM-dd-yyyy HH:mm:ss').alias('custom_input_timestamp_utc'),
                         from_unixtime(col("Update_Timestamp_UTC"), 'MM-dd-yyyy').alias('custom_last_update_timestamp_utc'),
                         from_unixtime(col("Input_Timestamp_UTC")).cast("timestamp").alias('default_ts_input_timestamp_utc'),
                         from_unixtime(col("Update_Timestamp_UTC")).cast("timestamp").alias('default_ts_update_timestamp_utc')
                        )
display(df_cust.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### to_timestamp
# MAGIC
# MAGIC - Convert **String** to **Timestamp** type.
# MAGIC - **yyyy-MM-dd HH:mm:ss.SSS** is the **standard timestamp format**.
# MAGIC
# MAGIC **Syntax**
# MAGIC
# MAGIC        to_timestamp(column_name, pattern)

# COMMAND ----------

# DBTITLE 1,to_timestamp
df_all = df_cast.select(current_timestamp().alias("created_timestamp"),
                        expr("current_user()").alias("created_by"),
                        from_unixtime(col("Input_Timestamp_UTC")).alias('default_str_input_timestamp_utc'),
                        to_timestamp(from_unixtime(col("Input_Timestamp_UTC"))).alias('default_input_timestamp_utc'))
display(df_all.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC - **current_user()** is **not directly** available as a PySpark function.
# MAGIC - To resolve this issue, you can use the **SQL expression functionality** provided by PySpark to execute SQL functions that are **not directly** exposed in the **PySpark API**.