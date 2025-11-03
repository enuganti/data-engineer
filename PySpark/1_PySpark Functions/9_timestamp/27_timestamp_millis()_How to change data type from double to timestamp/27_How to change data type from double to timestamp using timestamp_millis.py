# Databricks notebook source
# MAGIC %md
# MAGIC #### **How to change data type from double to timestamp using timestamp_millis?**

# COMMAND ----------

# MAGIC %md
# MAGIC **timestamp_millis()**
# MAGIC
# MAGIC - This function is useful when you have **timestamps** represented as **long integers** in **milliseconds** and you need to convert them into a **human-readable timestamp format** for further processing or analysis.
# MAGIC
# MAGIC - For example, if you have the timestamp value **1230219000123** in **milliseconds**, using **timestamp_millis(1230219000123)** will convert this value to the **TIMESTAMP 2008-12-25 07:30:00.123**, representing the **date and time** in a readable format.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC       timestamp_millis(expr)
# MAGIC      
# MAGIC **expr:** An integral numeric expression specifying **milliseconds**.
# MAGIC
# MAGIC **Returns:** TIMESTAMP.
# MAGIC
# MAGIC       > SELECT timestamp_millis(1230219000123);
# MAGIC         2008-12-25 07:30:00.123

# COMMAND ----------

# DBTITLE 1,import required libraries
import pyspark.sql.functions as f
from pyspark.sql.functions import col, timestamp_millis

# COMMAND ----------

# DBTITLE 1,Read Source dataset
df = spark.read.csv("/FileStore/tables/doubletotimestamp-2.csv", header=True, inferSchema=True)
display(df.limit(10))

# COMMAND ----------

# DBTITLE 1,convert double to timestamp by timestamp_millis
# Convert columns from double (milliseconds) to timestamp using timestamp_millis
df_dbl = df\
    .withColumn("Start_Cust_Date", f.timestamp_millis(col("Start_Cust_Date")))\
    .withColumn("End_Date", f.timestamp_millis(col("End_Date")))\
    .withColumn("Updated_Date", f.timestamp_millis(col("Updated_Date")))\
    .withColumn("Last_Date_UTC", f.timestamp_millis(col("Last_Date_UTC")))
display(df_dbl)

# COMMAND ----------

# DBTITLE 1,convert double to timestamp by cast as Long
# Convert columns from double (milliseconds) to Long
# Convert columns from Long (milliseconds) to timestamp using timestamp_millis
df_lng = df\
    .withColumn("Start_Cust_Date", f.timestamp_millis(col("Start_Cust_Date").cast("long")))\
    .withColumn("End_Date", f.timestamp_millis(col("End_Date").cast("long")))\
    .withColumn("Updated_Date", f.timestamp_millis(col("Updated_Date").cast("long")))\
    .withColumn("Last_Date_UTC", f.timestamp_millis(col("Last_Date_UTC").cast("long")))
display(df_lng)
