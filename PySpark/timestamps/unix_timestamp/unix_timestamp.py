# Databricks notebook source
# MAGIC %md
# MAGIC #### **unix_timestamp (EPOCH)**
# MAGIC
# MAGIC - is used to get the **current time** and to convert the time string in format **yyyy-MM-dd HH:mm:ss** to Unix timestamp (in **seconds**) by using the **current timezone of the system**.
# MAGIC
# MAGIC - It is an **integer** and started from **January 1st 1970 Midnight UTC**.
# MAGIC
# MAGIC - Converts **Date and Timestamp** Column to **Unix Time**.
# MAGIC
# MAGIC - https://www.epochconverter.com/

# COMMAND ----------

# MAGIC %md
# MAGIC  **Syntax**
# MAGIC
# MAGIC      unix_timestamp(date_time_column, pattern)
# MAGIC
# MAGIC **Arguments**
# MAGIC
# MAGIC - **date_time_column:** An optional **DATE, TIMESTAMP, or a STRING** expression in a valid datetime format.
# MAGIC
# MAGIC - **pattern:** An optional STRING expression specifying the format if expr is a STRING.
# MAGIC
# MAGIC - The default **pattern** value is **'yyyy-MM-dd HH:mm:ss'**.
# MAGIC
# MAGIC **Returns:** BIGINT

# COMMAND ----------

# MAGIC %md
# MAGIC ![test image](files/syntax-1.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- unix timestamp or epoc time 
# MAGIC -- If no argument is provided the default is the current timestamp.
# MAGIC -- The number of seconds that are passed from 01-JAN-1970 to this particular moment.
# MAGIC SELECT unix_timestamp() AS default;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- convert epoc time to human readble format
# MAGIC SELECT from_unixtime(unix_timestamp()) AS default;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT unix_timestamp('2016-04-08', 'yyyy-MM-dd') AS unixtimestamp;

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import lit, col, unix_timestamp, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Convert timestamp string to Unix time**

# COMMAND ----------

df_ts_ut = spark.createDataFrame([
    (20140228, "2017-03-09 10:27:18", "17-01-2019 12:01:19", "01-07-2019 12:01:19", "08-18-2022", "2022-08-18"),
    (20160229, "2017-03-10 15:27:18", "13-05-2022 15:05:36", "05-13-2022 15:05:36", "04-29-2022", "2022-04-29"),
    (20171031, "2017-03-13 12:27:18", "18-08-2023 17:22:45", "08-18-2023 17:22:45", "08-22-2022", "2022-08-22"),
    (20191130, "2017-03-15 12:27:18", "22-04-2002 18:15:34", "04-22-2002 18:15:34", "03-28-2021", "2021-09-28"),
    (20221130, "2017-03-15 02:27:18", "29-06-2005 22:55:29", "06-29-2005 22:55:29", "02-13-2022", "2022-02-13"),
    (20321130, "2017-03-18 11:27:18", "20-10-2019 23:45:56", "10-20-2019 23:45:56", "07-23-2024", "2024-07-23")],
    ["dateid", "start_timestamp", "input_timestamp", "last_timestamp", "start_date", "end_date"])
display(df_ts_ut)

# COMMAND ----------

df_ts_ut = df_ts_ut\
         .withColumn('dateid', F.unix_timestamp(F.col('dateid').cast('string'),'yyyyMMdd'))\
         .withColumn('start_timestamp', F.unix_timestamp(F.col('start_timestamp')))\
         .withColumn('input_timestamp', F.unix_timestamp(F.col('input_timestamp'), 'dd-MM-yyyy HH:mm:ss'))\
         .withColumn('last_timestamp', F.unix_timestamp(F.col('last_timestamp'), "MM-dd-yyyy HH:mm:ss"))\
         .withColumn('start_date', F.unix_timestamp(F.col('start_date'), 'MM-dd-yyyy'))\
         .withColumn('end_date', F.unix_timestamp(F.col('end_date'), "yyyy-MM-dd"))
display(df_ts_ut)

# COMMAND ----------

df_ts_ut = df_ts_ut.select("*",
                           unix_timestamp(lit("2024-09-29 13:45:55")).alias('lit_timestamp'),\
                           unix_timestamp(current_timestamp(),'yyyy-MM-dd HH:mm:ss').alias('current_time'),\
                           unix_timestamp(current_timestamp(),'yyyy-MM-dd').alias('current_date'),\
                           unix_timestamp().alias('default_time')
                           )
display(df_ts_ut)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Convert string to timestamp to Unix time**

# COMMAND ----------

# MAGIC %md
# MAGIC **convert string to timestamp by cast()**

# COMMAND ----------

# Create a DataFrame from the timestamps list
timestamps = ["2024-01-15T07:17:37Z", "2024-01-16T10:28:33Z", "2024-01-15T07:17:21Z", "2024-01-16T10:12:49Z",
              "2024-01-16T10:36:48Z", "2024-01-16T11:44:29Z", "2024-01-15T07:58:03Z", "2024-01-15T07:16:18Z",
              "2024-01-16T10:27:13Z", "2024-01-16T10:10:34Z", "2024-01-16T10:39:04Z", "2024-01-20T23:39:04Z",
              "2024-01-21T07:39:04Z", "2024-01-16T11:44:29Z", "2024-01-15T07:17:21Z", "2024-01-16T10:36:48Z"
              ]

timestamps_df = spark.createDataFrame([(ts,) for ts in timestamps], ["time_zone"])
display(timestamps_df)

# COMMAND ----------

# Convert timestamp string to timestamp type
timestamps_df = timestamps_df.withColumn("time_zone", col("time_zone").cast("timestamp"))
display(timestamps_df)

# COMMAND ----------

timestamps_df = timestamps_df.withColumn('time_zone', F.unix_timestamp(F.col('time_zone'), "yyyy-MM-dd HH:mm:ss"))
display(timestamps_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **convert string to timestamp by to_timestamp()**

# COMMAND ----------

data = [
    ("2022-08-08","2022-04-17 17:16:20","19-04-2022 23:02:32"),
    ("2022-04-29","2022-11-07 04:03:11","27-07-2022 18:09:39"),
    ("2022-08-22","2022-02-07 09:15:31","08-11-2022 09:58:34"),
    ("2021-12-28","2022-02-28 02:47:25","03-01-2022 01:59:22"),
    ("2022-02-13","2022-05-22 11:25:29","25-02-2022 04:46:47")
    ]
 
df_stt = spark.createDataFrame(data, schema=["start_date","input_timestamp","update_timestamp"])
display(df_stt)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
df_stt = df_stt.withColumn("input_timestamp", to_timestamp("input_timestamp", 'yyyy-MM-dd HH:mm:ss'))\
               .withColumn("update_timestamp", to_timestamp("update_timestamp", 'dd-MM-yyyy HH:mm:ss'))
display(df_stt)

# COMMAND ----------

df_stt = df_stt.withColumn('start_date', F.unix_timestamp(F.col('start_date'), "yyyy-MM-dd"))\
               .withColumn('input_timestamp', F.unix_timestamp(F.col('input_timestamp'), "yyyy-MM-dd HH:mm:ss"))\
               .withColumn('update_timestamp', F.unix_timestamp(F.col('update_timestamp'), "yyyy-MM-dd HH:mm:ss"))
display(df_stt)
