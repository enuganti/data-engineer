# Databricks notebook source
# MAGIC %md
# MAGIC ##### to_timestamp:
# MAGIC
# MAGIC - How to Convert `date string` to `timestamp` using `to_timestamp`?
# MAGIC - How to Convert `date` to `timestamp` using `to_timestamp`?
# MAGIC
# MAGIC - **yyyy-MM-dd HH:mm:ss.SSS** is the **standard timestamp format**.
# MAGIC
# MAGIC - **Syntax**
# MAGIC
# MAGIC        to_timestamp(column_name, pattern)

# COMMAND ----------

# MAGIC %md
# MAGIC - If your **date** is in the **default format yyyy-MM-dd**, you can directly use:
# MAGIC
# MAGIC       df_ts = df.withColumn("my_timestamp", F.to_timestamp(F.col("my_date")))
# MAGIC       df_ts.show(truncate=False)
# MAGIC
# MAGIC - Spark assumes **midnight time (00:00:00)** because date doesn’t have time info.
# MAGIC
# MAGIC       +----------+-------------------+
# MAGIC       |my_date   |my_timestamp       |
# MAGIC       +----------+-------------------+
# MAGIC       |2025-11-07|2025-11-07 00:00:00|
# MAGIC       |2025-12-25|2025-12-25 00:00:00|
# MAGIC       |2024-02-29|2024-02-29 00:00:00|
# MAGIC       +----------+-------------------+

# COMMAND ----------

# MAGIC %skip
# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

df_ts = spark.read.csv("/Volumes/@azureadb/pyspark/timestamp/to_timestamp.csv", header=True, inferSchema=True)
display(df_ts.limit(10))

# COMMAND ----------

from pyspark.sql.functions import lit, col, to_date, to_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) How to Convert `date string` to `timestamp` using `to_timestamp`?
# MAGIC - Convert date (data type: string) to timestamp

# COMMAND ----------

# DBTITLE 1,columns format: date string
df_ts = df_ts.withColumn("Start_Date", to_timestamp(col("Start_Date"),'dd-MMM-yy'))\
             .withColumn("Last_Date", to_timestamp(col('Last_Date'),'dd-MMM-yy'))\
             .withColumn("Delivery_End_Date", to_timestamp(col('Delivery_End_Date'),'dd-MM-yyyy'))\
             .withColumn("Pricing_Date", to_timestamp(col("Pricing_Date"), 'dd-MMM-yy'))

display(df_ts.limit(10))

# COMMAND ----------

df_ts = df_ts.withColumn("to_date", to_timestamp(lit('02-Mar-2021'), 'dd-MMM-yyyy'))
display(df_ts.limit(10))

# COMMAND ----------

df_ts = df_ts.withColumn("to_timestamp", to_timestamp(lit('02-Mar-2021 17:30:15'), 'dd-MMM-yyyy HH:mm:ss'))
display(df_ts.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) How to Convert `date` to `timestamp` using `to_timestamp`?
# MAGIC - Convert date (data type: date) to timestamp

# COMMAND ----------

# DBTITLE 1,columns format: date (No need to define format)
df_ts = df_ts.withColumn("End_Date_ts", to_timestamp(col("End_Date")))\
             .withColumn("Expiration_Date_ts", to_timestamp(col('Expiration_Date')))\
             .withColumn("Input_Start_Date_ts", to_timestamp(col('Input_Start_Date')))\
             .withColumn("Input_End_Date_ts", to_timestamp(col("Input_End_Date")))\
             .withColumn("Delivery_Start_Date8_ts", to_timestamp(col("Delivery_Start_Date8")))\
             .withColumn("Delivery_Start_Date10_ts", to_timestamp(col("Delivery_Start_Date10")))

display(df_ts.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Convert timestamp (data type: string) to timestamp

# COMMAND ----------

datetimes = [(20140228, "28-Feb-2014 10:00:00.123"),
             (20160229, "20-Feb-2016 08:08:08.999"),
             (20171031, "31-Dec-2017 11:59:59.123"),
             (20191130, "31-Aug-2019 00:00:00.000")
             ]

datetimesDF = spark.createDataFrame(datetimes, schema="date BIGINT, time STRING")
display(datetimesDF)

# COMMAND ----------

datetimesDF = datetimesDF\
    .withColumn('to_date', to_date(col('date').cast('string'), 'yyyyMMdd'))\
    .withColumn('to_timestamp', to_timestamp(col('time'), 'dd-MMM-yyyy HH:mm:ss.SSS'))
 
display(datetimesDF)