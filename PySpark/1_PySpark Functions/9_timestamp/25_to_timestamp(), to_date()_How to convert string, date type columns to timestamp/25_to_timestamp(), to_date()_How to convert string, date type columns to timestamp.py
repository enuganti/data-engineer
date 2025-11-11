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
# MAGIC ##### Scenario 01
# MAGIC
# MAGIC - If your **date** is in the **default format (yyyy-MM-dd)**, you can directly use:
# MAGIC
# MAGIC       df.withColumn("my_timestamp", F.to_timestamp(F.col("my_date")))
# MAGIC
# MAGIC - Spark assumes **midnight time (00:00:00)** because date doesn’t have time info.
# MAGIC
# MAGIC |  my_date (STRING DATE / DATE)   |          my_timestamp            |
# MAGIC |---------------------------------|----------------------------------|
# MAGIC |      2025-11-07                 |   2025-11-07T00:00:00:000+00:00  |
# MAGIC |      2025-12-25                 |   2025-12-25T00:00:00:000+00:00  |
# MAGIC |      2024-02-29                 |   2024-02-29T00:00:00:000+00:00  |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Scenario 02
# MAGIC
# MAGIC - **Custom format (timestamp)**
# MAGIC
# MAGIC       df.withColumn('to_timestamp', to_timestamp(col('time'), 'dd-MMM-yyyy HH:mm:ss.SSS'))
# MAGIC
# MAGIC |  my_timestamp (STRING)          |          my_timestamp            |
# MAGIC |---------------------------------|----------------------------------|
# MAGIC |   28-Feb-2014 10:00:00.123      |   2014-02-28T10:00:00.123+00:00  |
# MAGIC |   20-Feb-2016 08:08:08.999      |   2016-02-20T08:08:08.999+00:00  |
# MAGIC |   31-Dec-2017 11:59:59.123      |   2017-12-31T11:59:59.123+00:00  |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Scenario 03
# MAGIC
# MAGIC - **Custom format (timestamp)**
# MAGIC
# MAGIC       df.withColumn("LOAD_DATE_TS", to_timestamp("Load_Date"))
# MAGIC
# MAGIC |  my_timestamp (STRING)          |          my_timestamp            |
# MAGIC |---------------------------------|----------------------------------|
# MAGIC |  2025-08-20 10:00:00	          |   2025-08-20T10:00:00.000+00:00  |
# MAGIC |  2025-08-20 12:00:00	          |   2025-08-20T12:00:00.000+00:00  |
# MAGIC |  2025-08-21 09:00:00	          |   2025-08-21T09:00:00.000+00:00  |
# MAGIC |  2025-08-21 15:00:00	          |   2025-08-21T15:00:00.000+00:00  |

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
df_ts = df_ts.withColumn("End_Date", to_timestamp(col("End_Date")))\
             .withColumn("Expiration_Date", to_timestamp(col('Expiration_Date')))\
             .withColumn("Input_Start_Date", to_timestamp(col('Input_Start_Date')))\
             .withColumn("Input_End_Date", to_timestamp(col("Input_End_Date")))\
             .withColumn("Delivery_Start_Date", to_timestamp(col("Delivery_Start_Date")))\
             .withColumn("Delivery_Last_Date", to_timestamp(col("Delivery_Last_Date")))

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

# COMMAND ----------

data = [("2025-08-20 10:00:00"),
        ("2025-08-20 12:00:00"),
        ("2025-08-21 09:00:00"),
        ("2025-08-21 15:00:00")
       ]

schema = ["Load_Date"]

df_ts = spark.createDataFrame(data, schema)
display(df_ts)

# COMMAND ----------

df_final_ts = df_ts.withColumn("LOAD_DATE_TS", to_timestamp("Load_Date"))
display(df_final_ts)