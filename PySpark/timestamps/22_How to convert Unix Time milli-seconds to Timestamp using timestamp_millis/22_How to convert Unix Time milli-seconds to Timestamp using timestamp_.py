# Databricks notebook source
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

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import LongType

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/timestamp_millis-3.csv", header=True, inferSchema=True)
display(df.limit(10))

# COMMAND ----------

df_err = df.select(current_timestamp().alias("created_timestamp"),
               current_user().alias("created_by"),
               timestamp_millis(col("Input_Timestamp_UTC")).alias('input_timestamp_utc'),
               timestamp_millis(col("Update_Timestamp_UTC")).alias('last_update_timestamp_utc'))
display(df_err.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC - The reason why you are not encountering this error in this notebook could be due to the difference in **Spark versions or configurations between the two environments**. It's possible that the version of Spark in the **Databricks Community Edition does not include the timestamp_millis function**.
# MAGIC
# MAGIC - The functions **timestamp_millis and unix_millis** are **not available** in the **Apache Spark DataFrame API**. These functions are **specific to SQL** and are included in **Spark 3.1.1 and above**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Solution 01**

# COMMAND ----------

# MAGIC %md
# MAGIC - This code snippet uses **from_unixtime(col / 1000)** to **convert the milliseconds to seconds** and then casts the result to a timestamp
# MAGIC
# MAGIC - The error occurs because **timestamp_millis** is not a **direct function** available in **pyspark.sql.functions**. To convert a timestamp in **milliseconds to a timestamp** type, you should use the **from_unixtime** function combined with **division by 1000** (since **from_unixtime expects seconds, not milliseconds**) and then **cast to a timestamp** if needed.

# COMMAND ----------

df1 = df.select(
    current_timestamp().alias("created_timestamp"),
    expr("current_user()").alias("created_by"),
    from_unixtime(col("Input_Timestamp_UTC") / 1000).alias('input_timestamp_utc'),
    from_unixtime(col("Update_Timestamp_UTC") / 1000).alias('last_update_timestamp_utc')
)

display(df1.limit(10))

# COMMAND ----------

df2 = df.select(
    current_timestamp().alias("created_timestamp"),
    expr("current_user()").alias("created_by"),
    to_timestamp(from_unixtime(col("Input_Timestamp_UTC") / 1000), 'yyyy-MM-dd HH:mm:ss').alias('input_timestamp_utc'),
    to_timestamp(from_unixtime(col("Update_Timestamp_UTC") / 1000), 'yyyy-MM-dd HH:mm:ss').alias('update_timestamp_utc')
)

display(df2.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC select timestamp_millis(1724256609000)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Solution 02**

# COMMAND ----------

df_exp = df.select(
    current_timestamp().alias("created_timestamp"),
    expr("current_user()").alias("created_by"),
    expr("timestamp_millis(Input_Timestamp_UTC)").alias('input_timestamp_utc'),
    expr("timestamp_millis(Update_Timestamp_UTC)").alias('last_update_timestamp_utc')
)
display(df_exp.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Solution 03**

# COMMAND ----------

# MAGIC %md
# MAGIC - The reason why you are not encountering this error in this notebook could be due to the difference in **Spark versions or configurations between the two environments**. It's possible that the version of Spark in the **Databricks Community Edition does not include the timestamp_millis function**.
# MAGIC
# MAGIC - The functions **timestamp_millis and unix_millis** are **not available** in the **Apache Spark DataFrame API**. These functions are **specific to SQL** and are included in **Spark 3.1.1 and above**.

# COMMAND ----------

df_confg = df.select(current_timestamp().alias("created_timestamp"),
               current_user().alias("created_by"),
               timestamp_millis(col("Input_Timestamp_UTC")).alias('input_timestamp_utc'),
               timestamp_millis(col("Update_Timestamp_UTC")).alias('last_update_timestamp_utc'))
display(df_confg.limit(10))
