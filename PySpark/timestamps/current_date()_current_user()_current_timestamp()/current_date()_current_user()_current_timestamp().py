# Databricks notebook source
# MAGIC %md
# MAGIC - How to add **meta columns** to **target tables** while moving data from **raw to enriched**.

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/current_date___current_user___current_timestamp__.csv", header=True, inferSchema=True)
display(df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import lit, expr, col, current_date, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### **1) current_date()**
# MAGIC
# MAGIC - Returns **current system date without time** in PySpark **DateType** which is in format **yyyy-MM-dd**.

# COMMAND ----------

df_cd = df.withColumn("curr_date", current_date())
display(df_cd.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **2) current_user()**
# MAGIC
# MAGIC **Syntax**
# MAGIC
# MAGIC      current_user()
# MAGIC
# MAGIC **Arguments**
# MAGIC - This function takes no arguments.
# MAGIC
# MAGIC **Return**
# MAGIC - STRING

# COMMAND ----------

from pyspark.sql.functions import current_user

# COMMAND ----------

# MAGIC %md
# MAGIC      df_cu = df_cd.withColumn("meta_created_by", current_user())
# MAGIC      display(df_cu.limit(10))

# COMMAND ----------

df_cu = df_cd.withColumn("meta_created_by", expr("current_user()"))
display(df_cu.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **3) current_timestamp()**
# MAGIC
# MAGIC - Returns **current system date & timestamp** in PySpark **TimestampType** which is in format **yyyy-MM-dd HH:mm:ss.SSS**.

# COMMAND ----------

df_ct = df_cd.withColumn("created_datetime", current_timestamp())\
             .withColumn("meta_created_timestamp", current_timestamp())\
             .withColumn("updated_datetime", current_timestamp())
display(df_ct.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **4) Hour() | Minute() | Second()**
# MAGIC
# MAGIC How to extract or get an hour, minute and second from a Spark timestamp column?
# MAGIC
# MAGIC - Spark functions provides **hour(), minute() and second()** functions to extract **hour, minute and second** from **Timestamp column** respectively.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **hour:** 
# MAGIC - function hour() extracts hour unit from Timestamp column or string column containing a timestamp.
# MAGIC
# MAGIC  **Syntax**
# MAGIC
# MAGIC       hour(expr)
# MAGIC  **Arguments**
# MAGIC  **expr:** A TIMESTAMP expression
# MAGIC
# MAGIC  **Returns**
# MAGIC  INTEGER

# COMMAND ----------

# MAGIC %md
# MAGIC #### **minute:** 
# MAGIC - function minute() extracts minute unit from Timestamp column or string column containing a timestamp.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **second:** 
# MAGIC - function second() extracts the second unit from the Timestamp column or string column containing a timestamp.

# COMMAND ----------

from pyspark.sql.functions import hour, minute, second

# COMMAND ----------

df_ct = df_ct.select("updated_datetime")\
             .withColumn("hour", hour(df_ct.updated_datetime))\
             .withColumn("minute", minute(df_ct.updated_datetime))\
             .withColumn("second", second(df_ct.updated_datetime))

display(df_ct.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Spark SQL**

# COMMAND ----------

spark.sql("select current_date(), current_timestamp()").show(truncate=False)

# COMMAND ----------

spark.sql("select date_format(current_date(),'MM-dd-yyyy') as date_format ," + \
          "to_timestamp(current_timestamp(),'MM-dd-yyyy HH mm ss SSS') as to_timestamp") \
     .show(truncate=False)
