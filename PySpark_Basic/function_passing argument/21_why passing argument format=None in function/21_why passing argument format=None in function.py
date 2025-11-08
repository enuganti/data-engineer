# Databricks notebook source
# MAGIC %md
# MAGIC ##### Why need to pass argument (format=None) in a function?

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

def data_fun(df, column, format=None):
    if format:
        df = df.withColumn(column, F.to_date(F.col(column), format))
    else:
        df = df.withColumn(column, F.to_date(F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC **1) When format=None**
# MAGIC
# MAGIC - **format=None => optional**
# MAGIC   - If **None**, Spark will use its **default formats**.
# MAGIC - Use this when your **data** is already in Spark’s **default expected format**:
# MAGIC   - For **dates** → **yyyy-MM-dd**
# MAGIC   - For **timestamps** → **yyyy-MM-dd HH:mm:ss**

# COMMAND ----------

# DBTITLE 1,dates → yyyy-MM-dd
data = [("2025-09-24",),
        ("2025-01-01",),
        ("2025-03-14",),
        ("2025-04-11",),
        ("2024-03-25",)]
        
df_dt = spark.createDataFrame(data, ["my_date"])

# No format needed (already yyyy-MM-dd)
df_dt_casted = data_fun(df_dt, "my_date", None)
display(df_dt_casted)

# COMMAND ----------

# MAGIC %md
# MAGIC - Works fine because Spark knows how to **parse 2025-09-24 without a custom format**.

# COMMAND ----------

# DBTITLE 1,timestamps → yyyy-MM-dd HH:mm:ss
data = [("2025-09-24 12:30:45",),
        ("2025-01-01 19:47:53",),
        ("2025-09-24 17:30:45",),
        ("2025-04-21 23:55:45",),
        ("2025-05-18 15:35:45",)]

df_ts = spark.createDataFrame(data, ["my_timestamp"])
display(df_ts)

# For timestamps in default "yyyy-MM-dd HH:mm:ss"
df_ts_casted = data_fun(df_ts, "my_timestamp", None)
display(df_ts_casted)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) When format="..."**
# MAGIC
# MAGIC - Use this when your data is **not** in Spark’s **default format**.

# COMMAND ----------

# DBTITLE 1,date
data = [("24-09-2025",),
        ("01-01-2025",),
        ("20-08-2025",),
        ("21-11-2025",),
        ("15-04-2025",)]

df_dt_cust = spark.createDataFrame(data, ["my_date"])

# Needs custom format
df_dt_cust_casted = data_fun(df_dt_cust, "my_date", "dd-MM-yyyy")
display(df_dt_cust_casted)

# COMMAND ----------

# DBTITLE 1,timestamp
data = [("2025/09/24 10:15:30",),
        ("2025/01/01 23:59:59",),
        ("2024/11/14 15:25:39",),
        ("2023/05/23 20:49:44",),
        ("2022/07/18 23:59:24",)]

df_ts_cust = spark.createDataFrame(data, ["my_timestamp"])

# Needs custom format
df_ts_cust_casted = data_fun(df_ts_cust, "my_timestamp", "yyyy/MM/dd HH:mm:ss")
display(df_ts_cust_casted)

# COMMAND ----------

# MAGIC %md
# MAGIC **Summary:**
# MAGIC - **format=None** → when data is already in **standard** Spark format.
# MAGIC   - yyyy-MM-dd
# MAGIC   - yyyy-MM-dd HH:mm:ss
# MAGIC - **format="..."** → when data uses a **custom format**.
# MAGIC   - dd-MM-yyyy
# MAGIC   - MM/dd/yyyy
# MAGIC   - yyyy/MM/dd HH:mm:ss, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC **Case A**
# MAGIC - Using **format=None** with **correct default format**

# COMMAND ----------

data = [("2025-09-24",),
        ("2025-01-01",),
        ("2025-03-14",),
        ("2025-04-11",),
        ("2024-03-21",)]

df_dt = spark.createDataFrame(data, ["my_date"])

# No format needed (already yyyy-MM-dd)
df_dt_casted = data_fun(df_dt, "my_date", None)
display(df_dt_casted)

# COMMAND ----------

# MAGIC %md
# MAGIC - Works fine, because **input** matches Spark’s **default yyyy-MM-dd**

# COMMAND ----------

# MAGIC %md
# MAGIC **Case B**
# MAGIC - Using **format=None** with **wrong format**.

# COMMAND ----------

data = [("24-09-2025",),
        ("01-01-2025",),
        ("20-08-2025",),
        ("21-11-2025",),
        ("15-04-2025",)]

df_dt_cust = spark.createDataFrame(data, ["my_date"])

# Needs custom format
df_dt_cust_cast = data_fun(df_dt_cust, "my_date", None)
display(df_dt_cust_cast)

# COMMAND ----------

# MAGIC %md
# MAGIC - Spark expected **yyyy-MM-dd**, but got **dd-MM-yyyy**.
# MAGIC - Since **24-09-2025 cannot be parsed**, it throughs **error**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Case C**
# MAGIC - Using **format="dd-MM-yyyy"**

# COMMAND ----------

df_cust_casted = data_fun(df_dt_cust, "my_date", "dd-MM-yyyy")
display(df_cust_casted)

# COMMAND ----------

# MAGIC %md
# MAGIC **Takeaway:**
# MAGIC - If you use **format=None** with data in **default format**, it works.
# MAGIC - If you use **format=None** with data in **non-standard format**, Spark converts it to **NULL**.
# MAGIC - To avoid this, always provide the **right format="..."** when your input doesn’t match the default.