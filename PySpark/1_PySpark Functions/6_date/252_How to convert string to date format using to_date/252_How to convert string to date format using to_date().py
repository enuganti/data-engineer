# Databricks notebook source
# MAGIC %md
# MAGIC ##### to_date()
# MAGIC
# MAGIC **How to convert string to date format?**
# MAGIC
# MAGIC ✅ **to_date()** function is used to format a **date string / timestamp string column** into the **Date Type column** using a **specified format**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax:**
# MAGIC
# MAGIC      to_date(column,format)
# MAGIC      to_date(col("string_column"),"MM-dd-yyyy") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### to_date() Vs date_format()

# COMMAND ----------

# MAGIC %md
# MAGIC **1) to_date()**
# MAGIC
# MAGIC ✅ to_date() function is used to format **string (StringType) to date (DateType)** column.
# MAGIC
# MAGIC ✅ If the **format is not provide**, to_date() takes the **default value as 'yyyy-MM-dd'**.
# MAGIC
# MAGIC ✅ to_date() accepts the first argument in any date format.
# MAGIC
# MAGIC ✅ If the input column values **does not match** with the format specified (second argument) then to_date() populates the new column with **null**.
# MAGIC
# MAGIC ✅ Returns **NULL** if the format does **not match**.
# MAGIC
# MAGIC ✅ Extracts only the **date** portion **(removes time part if present)**.

# COMMAND ----------

# MAGIC %md
# MAGIC **2) date_format()**
# MAGIC
# MAGIC ✅ date_format() function is used to **format date (DateType / StringType) to (StringType)** in the specified format.
# MAGIC
# MAGIC ✅ If the format is **not provide**, date_format() throws a **TypeError**.
# MAGIC
# MAGIC ✅ date_format() requires the **first argument** to be in **'yyyy-MM-dd'** format, else it populates the **new column with null**.
# MAGIC
# MAGIC ✅ If the **input column values does not match** with the **format** specified (second argument) then date_format() converts it in the specified **format**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Content:**
# MAGIC - How to Convert `string date`, `string timestamp` to `date`?
# MAGIC - How to convert `string timestamp` to `date`?
# MAGIC - spark sql

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables 

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/to_date.csv", header=True, inferSchema=True)
display(df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import lit, col, to_date, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) How to Convert `string date`, `string timestamp` to `date`?

# COMMAND ----------

df_date = df.withColumn("input_timestamp", to_date(col("input_timestamp"),'dd/MM/yyyy H:mm').alias("input_timestamp"))\
            .withColumn("Effective_Date", to_date(col("Effective_Date"),'d-MMM-yy').alias("effective_date"))\
            .withColumn("pymt_timestamp", to_date(col("pymt_timestamp"),'dd/MM/yyyy H').alias("pymt_timestamp"))
display(df_date.limit(10))

# COMMAND ----------

# Custom Timestamp format to DateType
df_date = df_date.withColumn("custom_timestamp", to_date(lit('06-24-2019 12:01:19.000'),'MM-dd-yyyy HH:mm:ss.SSSS'))
display(df_date.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) How to convert `string timestamp` to `date`?

# COMMAND ----------

data = [(1,"HSR","2021-07-24 12:01:19.335"),
        (2,"BDA","2019-07-22 13:02:20.220"),
        (3,"BMRDS","2021-07-25 03:03:13.098"),
        (4,"APSRTC","2023-09-25 15:33:43.054"),
        (5,"SDARC","2024-05-25 23:53:53.023")]

schema = ["id","Name","input_timestamp"]

df_ex = spark.createDataFrame(data, schema)
display(df_ex)

# COMMAND ----------

# Timestamp String to DateType
df_ex = df_ex.withColumn("input_timestamp",to_date("input_timestamp"))\
             .withColumn("current_date",to_date(current_timestamp()))
display(df_ex)

# COMMAND ----------

# MAGIC %md
# MAGIC **spark sql**

# COMMAND ----------

# SQL TimestampType to DateType
spark.sql("select to_date(current_timestamp) as date_type").show()

# COMMAND ----------

spark.sql("select to_date('02-03-2013','MM-dd-yyyy') date").show()

# COMMAND ----------

# QL CAST TimestampType to DateType
spark.sql("select date(to_timestamp('2019-06-24 12:01:19.000')) as date_type").show()

# COMMAND ----------

# SQL CAST timestamp string to DateType
spark.sql("select date('2019-06-24 12:01:19.000') as date_type").show()

# COMMAND ----------

# SQL Timestamp String (default format) to DateType
spark.sql("select to_date('2019-06-24 12:01:19.000') as date_type").show()

# COMMAND ----------

# SQL Custom Timeformat to DateType
spark.sql("select to_date('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as date_type").show()