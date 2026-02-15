# Databricks notebook source
# MAGIC %md
# MAGIC **DATE_FORMAT()**
# MAGIC
# MAGIC - This function allows you to **convert date and timestamp** columns into a specified **string** format.
# MAGIC
# MAGIC - In order to use DATE_FORMAT() function, your column should be either in **'date' format or 'timestamp' format**.
# MAGIC
# MAGIC -  Format a date column into a specific pattern you need, like changing **2024-09-05** into **05-Sep-2024** or any format you prefer. 

# COMMAND ----------

# MAGIC %md
# MAGIC **Content:**
# MAGIC - convert `date` to `string date`
# MAGIC - convert `string timestamp` to `timestamp`
# MAGIC - convert `timestamp` to `string timestamp`
# MAGIC - Format `date` columns using `date_format`
# MAGIC - convert `date` to `string date` type

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/date_format-2.csv", header=True, inferSchema=True)
display(df.limit(10))

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import col, to_timestamp, date_format, current_date, current_timestamp, to_date

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Convert `date` to `string date`
# MAGIC **Last_transaction_date**

# COMMAND ----------

df_ltd = df.withColumn("Last_transaction_date",date_format("Last_transaction_date","yyyy-MM-dd"))
display(df_ltd.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Convert `string timestamp` to `timestamp`
# MAGIC
# MAGIC - input_timestamp
# MAGIC - Last_transaction_date
# MAGIC - Effective_Date
# MAGIC - last_timestamp
# MAGIC - pymt_timestamp

# COMMAND ----------

# convert input_timestamp, Last_transaction_date, Effective_Date, last_timestamp, pymt_timestamp into timestamp
df_all = df.withColumn("input_timestamp",to_timestamp("input_timestamp","dd/MM/yyyy H:mm"))\
           .withColumn("Last_transaction_date",to_timestamp("Last_transaction_date","yyyy-MM-dd"))\
           .withColumn("Effective_Date",to_timestamp("Effective_Date","d-MMM-yy"))\
           .withColumn("last_timestamp",to_timestamp("last_timestamp","dd/MM/yyyy HH:mm:ss"))\
           .withColumn("pymt_timestamp",to_timestamp("pymt_timestamp","dd/MM/yyyy H"))    
display(df_all.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_timestamp("25/04/2023 24:56:18", 'dd/MM/yyyy HH:mm:ss')

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_timestamp("25/04/2023 23:56:18", 'dd/MM/yyyy HH:mm:ss')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Convert `timestamp` to `string timestamp`

# COMMAND ----------

# convert input_timestamp, Last_transaction_date, Effective_Date, last_timestamp, pymt_timestamp into timestamp
df_tmstpm_stg = df_all.withColumn("input_timestamp",date_format("input_timestamp","yyyy/MM/dd HH:mm:ss"))\
                      .withColumn("Last_transaction_date",date_format("Last_transaction_date","yyyy/MM/dd HH:mm:ss"))\
                      .withColumn("Effective_Date",date_format("Effective_Date","yyyy/MM/dd HH:mm:ss"))\
                      .withColumn("last_timestamp",date_format("last_timestamp","yyyy/MM/dd HH:mm:ss"))\
                      .withColumn("pymt_timestamp",date_format("pymt_timestamp","yyyy/MM/dd HH:mm:ss"))    
display(df_tmstpm_stg.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Format `date` columns using `date_format`

# COMMAND ----------

# Using date_format()
df_format = df_tmstpm_stg.select(current_date().alias("current_date"), \
                           date_format(current_timestamp(),"yyyy MM dd").alias("yyyy MM dd"), \
                           date_format(current_timestamp(),"MM/dd/yyyy hh:mm").alias("MM/dd/yyyy"), \
                           date_format(current_timestamp(),"yyyy MMM dd").alias("yyyy MMMM dd"), \
                           date_format(current_timestamp(),"yyyy MMMM dd E").alias("yyyy MMMM dd E")
                           )
display(df_format.limit(10))

# COMMAND ----------

# SQL
spark.sql("select current_date() as current_date, "+
      "date_format(current_timestamp(),'yyyy MM dd') as yyyy_MM_dd, "+
      "date_format(current_timestamp(),'MM/dd/yyyy hh:mm') as MM_dd_yyyy, "+
      "date_format(current_timestamp(),'yyyy MMM dd') as yyyy_MMMM_dd, "+
      "date_format(current_timestamp(),'yyyy MMMM dd E') as yyyy_MMMM_dd_E").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) convert `date` to `string date` type

# COMMAND ----------

data = spark.createDataFrame([('05/22/2022', '10/21/2022')], schema=['Input_Timestamp', 'Last_Timestamp'])
display(data)

# COMMAND ----------

# to_date() converts "date string" into "date". We need to specify format of date in the string in the function
# convert string to date
string_date = data \
    .withColumn('Input_Timestamp', to_date(col("Input_Timestamp"), "MM/dd/yyyy")) \
    .withColumn('Last_Timestamp', to_date(col("Last_Timestamp"), "MM/dd/yyyy"))

display(string_date)

# COMMAND ----------

# convert date to string
date_string = string_date \
    .withColumn('Input_Timestamp', date_format(col("Input_Timestamp"), "dd-MM-yyyy")) \
    .withColumn('Last_Timestamp', date_format(col("Last_Timestamp"), "dd-MM-yyyy"))

display(date_string)

# COMMAND ----------

# Assuming 'data' is your DataFrame
date_format_corrected = data \
    .withColumn('Input_Timestamp', date_format(to_date(col("Input_Timestamp"), "MM/dd/yyyy"), "dd-MM-yyyy")) \
    .withColumn('Last_Timestamp', date_format(to_date(col("Last_Timestamp"), "MM/dd/yyyy"), "dd-MM-yyyy"))

display(date_format_corrected)