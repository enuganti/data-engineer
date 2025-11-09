# Databricks notebook source
# MAGIC %md
# MAGIC ##### why passing argument (format=None & is_timestamp=False) in function?

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

def data_fun_ts(df, column, format=None, is_timestamp=False):
    if is_timestamp:
        if format:
            df = df.withColumn(column, F.to_timestamp(F.col(column), format))
        else:
            df = df.withColumn(column, F.to_timestamp(F.col(column)))
    else:
        if format:
            df = df.withColumn(column, F.to_date(F.col(column), format))
        else:
            df = df.withColumn(column, F.to_date(F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC - **format=None => optional**
# MAGIC   - A **date/timestamp** format string (e.g., **"dd-MM-yyyy"**).
# MAGIC   - If **None**, Spark will use its **default formats**.
# MAGIC
# MAGIC   - Use this when your **data** is already in Spark’s **default expected format**:
# MAGIC     - For **dates** → **yyyy-MM-dd**
# MAGIC     - For **timestamps** → **yyyy-MM-dd HH:mm:ss**
# MAGIC
# MAGIC - **is_timestamp (boolean):**
# MAGIC   - **False** → treat the column as a **date**
# MAGIC   - **True** → treat the column as a **timestamp**

# COMMAND ----------

# MAGIC %md
# MAGIC **If is_timestamp=True**
# MAGIC
# MAGIC       if is_timestamp:
# MAGIC           if format:
# MAGIC               df = df.withColumn(column, F.to_timestamp(F.col(column), format))
# MAGIC           else:
# MAGIC               df = df.withColumn(column, F.to_timestamp(F.col(column)))
# MAGIC
# MAGIC - Uses **F.to_timestamp()** to convert the column into a **timestamp type**.
# MAGIC - If **format** is provided → Spark parses using that **format**.
# MAGIC - If **format=None** → Spark expects **default timestamp format**:
# MAGIC   - yyyy-MM-dd HH:mm:ss
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **If is_timestamp=False (default case)**
# MAGIC
# MAGIC       else:
# MAGIC           if format:
# MAGIC               df = df.withColumn(column, F.to_date(F.col(column), format))
# MAGIC           else:
# MAGIC               df = df.withColumn(column, F.to_date(F.col(column)))
# MAGIC
# MAGIC - Uses **F.to_date()** to convert the column into a **date type**.
# MAGIC - If **format** is provided → Spark parses using that **format**.
# MAGIC - If **format=None** → Spark expects **default date format**:
# MAGIC   - yyyy-MM-dd

# COMMAND ----------

# MAGIC %md
# MAGIC **Case A**
# MAGIC - **format=None**
# MAGIC - **default timestamp format**

# COMMAND ----------

# MAGIC %md
# MAGIC      df_casted = data_fun_ts(df, "my_ts", is_timestamp=True)
# MAGIC                           (or)
# MAGIC      df_casted = data_fun_ts(df, "my_ts", None, is_timestamp=True)
# MAGIC                           (or)
# MAGIC      df_casted = data_fun_ts(df, "my_ts", format=None, is_timestamp=True)

# COMMAND ----------

data = [("2025-09-24 12:30:45",),
        ("2025-01-01 11:43:55",),
        ("2025-08-20 14:35:35",),
        ("2025-05-08 09:12:45",),
        ("2025-07-18 06:18:25",)]

df_ts_nn = spark.createDataFrame(data, ["my_ts"])

df_ts_nn_cast = data_fun_ts(df_ts_nn, "my_ts", None, is_timestamp=True)
display(df_ts_nn_cast)

# COMMAND ----------

# MAGIC %md
# MAGIC - Works fine because Spark expects **yyyy-MM-dd HH:mm:ss**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Case B**
# MAGIC - **Custom timestamp or milliseconds format**

# COMMAND ----------

data = [("2025-09-24T12:30:45.123+05:30",),
        ("2025-10-14T15:35:55.163+05:30",),
        ("2025-11-11T16:39:55.184+05:30",),
        ("2025-05-04T23:55:25.193+05:30",),
        ("2024-02-16T21:26:38.143+05:30",)]

df_mls = spark.createDataFrame(data, ["my_ts"])

df_ts_cust_frmt_cast = data_fun_ts(df_mls, "my_ts", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", is_timestamp=True)
display(df_ts_cust_frmt_cast)

# COMMAND ----------

# MAGIC %md
# MAGIC       2025-09-24T12:30:45.123+05:30 -> is 5 hours 30 minutes ahead of UTC.
# MAGIC
# MAGIC       # convert to UTC
# MAGIC       12:30:45.123 - 05:30 = 07:00:45.123+00:00
# MAGIC
# MAGIC       Local time = 12:30:45.123
# MAGIC       Time zone = +05:30 (India Standard Time)

# COMMAND ----------

# MAGIC %md
# MAGIC | Input (IST)                   | Stored in Spark (UTC)         |
# MAGIC | ----------------------------- | ----------------------------- |
# MAGIC | 2025-09-24T12:30:45.123+05:30 | 2025-09-24T07:00:45.123+00:00 |
# MAGIC | 2025-10-14T15:35:55.163+05:30 | 2025-10-14T10:05:55.163+00:00 |
# MAGIC | 2025-11-11T16:39:55.184+05:30 | 2025-11-11T11:09:55.184+00:00 |
# MAGIC | 2025-05-04T23:55:25.193+05:30 | 2025-05-04T18:25:25.193+00:00 |
# MAGIC | 2024-02-16T21:26:38.143+05:30 | 2024-02-16T15:56:38.143+00:00 |

# COMMAND ----------

data = [
    ("2025-09-24T12:30:45",),
    ("2025-10-14T15:35:55",),
    ("2025-11-11T16:39:55",),
    ("2025-05-04T23:55:25",),
    ("2024-02-16T21:26:38",)
]

df_ts_cu = spark.createDataFrame(data, ["my_ts"])

# Correct ISO timestamp format
df_ts_cu_frmt_cast = data_fun_ts(df_ts_cu, "my_ts", format="yyyy-MM-dd'T'HH:mm:ss", is_timestamp=True)

display(df_ts_cu_frmt_cast)

# COMMAND ----------

data = [
    ("2025-09-24 12:30:45",),
    ("2025-10-14 15:35:55",),
    ("2025-11-11 16:39:55",),
    ("2025-05-04 23:55:25",),
    ("2024-02-16 21:26:38",)
]

df_ts_cu_t = spark.createDataFrame(data, ["my_ts"])

# Correct ISO timestamp format
df_ts_cu_t_frmt_cast = data_fun_ts(df_ts_cu_t, "my_ts", format="yyyy-MM-dd HH:mm:ss", is_timestamp=True)

display(df_ts_cu_t_frmt_cast)

# COMMAND ----------

# MAGIC %md
# MAGIC **Case C**
# MAGIC - **format="yyyy-MM-dd"**
# MAGIC - is_timestamp=True / False

# COMMAND ----------

data = [("2025-09-24",),
        ("2025-01-01",),
        ("2025-07-20",),
        ("2025-06-11",),
        ("2025-04-28",)]

df_frmt_dt = spark.createDataFrame(data, ["my_ts"])

df_frmt_ts_casted = data_fun_ts(df_frmt_dt, "my_ts", "yyyy-MM-dd", is_timestamp=False)
display(df_frmt_ts_casted)

# COMMAND ----------

data = [("2025-09-24",),
        ("2025-01-01",),
        ("2025-07-20",),
        ("2025-06-11",),
        ("2025-04-28",)]

df_frmt_dt = spark.createDataFrame(data, ["my_ts"])

df_frmt_dt_casted = data_fun_ts(df_frmt_dt, "my_ts", "yyyy-MM-dd", is_timestamp=True)
display(df_frmt_dt_casted)

# COMMAND ----------

# MAGIC %md
# MAGIC - Spark adds a **default 00:00:00** time when **only the date** is provided.

# COMMAND ----------

# MAGIC %md
# MAGIC **Case D**
# MAGIC - **format=None** with **only a date** (missing time)

# COMMAND ----------

data = [("2025-09-24",),
        ("2025-01-01",),
        ("2025-07-20",),
        ("2025-06-11",),
        ("2025-04-28",)]

df_ts_dt = spark.createDataFrame(data, ["my_ts"])

df_ts_dt_f_cast = data_fun_ts(df_ts_dt, "my_ts", None, is_timestamp=False)
display(df_ts_dt_f_cast)

# COMMAND ----------

data = [("2025-09-24",),
        ("2025-01-01",),
        ("2025-07-20",),
        ("2025-06-11",),
        ("2025-04-28",)]

df_ts_dt = spark.createDataFrame(data, ["my_ts"])

df_ts_dt_cast = data_fun_ts(df_ts_dt, "my_ts", None, is_timestamp=True)
display(df_ts_dt_cast)

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

# MAGIC %md
# MAGIC **Summary for timestamps**
# MAGIC - **format=None** → Works only if column is **already yyyy-MM-dd HH:mm:ss**.
# MAGIC - **format="yyyy-MM-dd"** → Parses date-only strings, time defaults to **midnight**.
# MAGIC - **Custom formats** (e.g., ISO with T, milliseconds, timezone) require **explicit format="..."**.