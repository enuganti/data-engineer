# Databricks notebook source
# MAGIC %md
# MAGIC #### **to_timestamp**
# MAGIC
# MAGIC - Convert **String** to **Timestamp** type.
# MAGIC
# MAGIC - **yyyy-MM-dd HH:mm:ss.SSS** is the **standard timestamp format**.
# MAGIC
# MAGIC - **Syntax**
# MAGIC
# MAGIC        to_timestamp(column_name, pattern)

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

df_ts = spark.read.csv("dbfs:/FileStore/tables/to_timestamp-3.csv", header=True, inferSchema=True)
display(df_ts.limit(10))

# COMMAND ----------

from pyspark.sql.functions import lit, col, to_date, to_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Convert date (data type: string) to timestamp**

# COMMAND ----------

df_ts = df_ts.withColumn("Start_Date", to_timestamp(col("Start_Date"),'dd-MMM-yy').alias('deal_start_date'))\
             .withColumn("Last_Date", to_timestamp(col('Last_Date'),'dd-MMM-yy').alias('last_date'))\
             .withColumn("Delivery_End_Date", to_timestamp(col('Delivery_End_Date'),'d/M/yyyy').alias('delivery_end_date'))\
             .withColumn("Pricing_Date", to_timestamp(col("Pricing_Date"), 'dd-MMM-yy').alias('pricing_date'))

display(df_ts.limit(10))

# COMMAND ----------

df_ts = df_ts.withColumn("to_date", to_timestamp(lit('02-Mar-2021'), 'dd-MMM-yyyy'))
display(df_ts.limit(10))

# COMMAND ----------

df_ts = df_ts.withColumn("to_timestamp", to_timestamp(lit('02-Mar-2021 17:30:15'), 'dd-MMM-yyyy HH:mm:ss'))
display(df_ts.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Convert date (data type: date) to timestamp**

# COMMAND ----------

df_ts = df_ts.withColumn("End_Date_ts", to_timestamp(col("End_Date").alias('end_date')))\
             .withColumn("Expiration_Date_ts", to_timestamp(col('Expiration_Date').alias('expiration_date')))\
             .withColumn("Input_Start_Date_ts", to_timestamp(col('Input_Start_Date').alias('input_start_date')))\
             .withColumn("Input_End_Date_ts", to_timestamp(col("Input_End_Date").alias('input_end_date')))\
             .withColumn("Delivery_Start_Date8_ts", to_timestamp(col("Delivery_Start_Date8").alias('delivery_start_date8')))\
             .withColumn("Delivery_Start_Date10_ts", to_timestamp(col("Delivery_Start_Date10").alias('delivery_start_date10')))

display(df_ts.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Convert timestamp (data type: string) to timestamp**

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
