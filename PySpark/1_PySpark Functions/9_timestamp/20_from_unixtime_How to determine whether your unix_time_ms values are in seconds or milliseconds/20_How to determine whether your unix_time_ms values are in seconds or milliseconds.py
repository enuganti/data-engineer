# Databricks notebook source
# MAGIC %md
# MAGIC #### How to determine whether your unix_time_ms values are in seconds or milliseconds?

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Check the Length or Magnitude of the Number**
# MAGIC
# MAGIC | Value Type       | Typical Range                  | Example         |
# MAGIC | ---------------- | ------------------------------ | --------------- |
# MAGIC | **Seconds**      | 10-digit numbers (≈ billions)  | `1633072800`    |
# MAGIC | **Milliseconds** | 13-digit numbers (≈ trillions) | `1633072800000` |

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Rule of Thumb:
# MAGIC
# MAGIC - If the value is **around 10 digits** → it's likely **seconds**
# MAGIC - If the value is **around 13 digits** → it's likely **milliseconds**

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Convert a Sample Value and Check Output

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, col

data_milli_seconds = [(1633072800000,)]
data_seconds = [(1633072800,)]

df_milli_seconds = spark.createDataFrame(data_milli_seconds, ["timestamp_milli_seconds"])
df_seconds = spark.createDataFrame(data_seconds, ["timestamp_seconds"])

# Convert both as if they were in seconds
df_timestamp_milli_seconds = df_milli_seconds.withColumn("as_milli_seconds", from_unixtime("timestamp_milli_seconds"))
display(df_timestamp_milli_seconds)

# Convert both as if they were in seconds
df_timestamp_seconds = df_seconds.withColumn("as_seconds", from_unixtime("timestamp_seconds"))
display(df_timestamp_seconds)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC |  timestamp_raw  |       as_seconds         |                      |
# MAGIC |-----------------|--------------------------|----------------------|
# MAGIC |  1633072800000  |  +53720-01-07 13:20:00   |  => clearly invalid  |
# MAGIC |  1633072800     |  2021-10-01 07:20:00     |  => makes sense      |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Divide by 1000 and Try Again

# COMMAND ----------

df_corrected_milli_seconds = df_milli_seconds.withColumn("timestamp_milli_sec", from_unixtime(col("timestamp_milli_seconds") / 1000))
df_corrected_seconds = df_seconds.withColumn("timestamp_sec", from_unixtime("timestamp_seconds"))
display(df_corrected_milli_seconds)
display(df_corrected_seconds)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Automatic safe conversion pattern

# COMMAND ----------

from pyspark.sql.functions import length, col, when, from_unixtime

# Example input data
data = [
    (1633072800000,),   # milliseconds
    (1622476800,),      # seconds
    (1609459200000,),   # milliseconds
    (1633068900,),      # seconds
    (1989456700000,),   # milliseconds
    (1622599900,),      # seconds
    (1689499800000,)    # milliseconds
]
df = spark.createDataFrame(data, ["unix_time_ms"])

# Column with detection & conversion
df_conv = df.withColumn(
    "ts_seconds",
    when(length(col("unix_time_ms")) >= 13, (col("unix_time_ms") / 1000).cast("long"))
    .otherwise(col("unix_time_ms").cast("long"))
).withColumn("ts_readable", from_unixtime("ts_seconds"))

display(df_conv)

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Summary:
# MAGIC
# MAGIC | What to Do                     | What It Tells You                                   |
# MAGIC | ------------------------------ | --------------------------------------------------- |
# MAGIC | Check if value has \~13 digits | Likely milliseconds                                 |
# MAGIC | Check if value has \~10 digits | Likely seconds                                      |
# MAGIC | Use `from_unixtime` and verify | If date looks wrong, likely wrong unit              |
# MAGIC | Try dividing by 1000           | Converts ms → sec; check if date looks correct then |