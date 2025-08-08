# Databricks notebook source
# MAGIC %md
# MAGIC #### **Question**
# MAGIC **How to convert custom date into timestamp?**

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_extract, col, from_unixtime, timestamp_millis, substring, length

# COMMAND ----------

data = [("/Date(1493596800000)/", "/Date(2840054400000)/", "/Date(1540857600000)/"),
        ("/Date(1537920000000)/", "/Date(2871676800000)/", "/Date(1540944000000)/"),
        ("/Date(1493510400000)/", "/Date(2871590400000)/", "/Date(1541376000000)/"),
        ("/Date(1522540800000)/", "/Date(1548028800000)/", "/Date(1541462400000)/"),
        ("/Date(1522540800000)/", "/Date(2840054400000)/", "/Date(1541548800000)/"),
        ("/Date(1493596800000)/", "/Date(2366755200000)/", "/Date(1541635200000)/")
        ]
columns = ["d1", "d2", "d3"]

df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**
# MAGIC - Using **from_unixtime + substring + cast**

# COMMAND ----------

df_substr = df\
.withColumn("d1_new", substring('d1', 7, length('d1')-8)) \
.withColumn("d2_new", substring('d2', 7, length('d2')-8)) \
.withColumn("d3_new", substring('d3', 7, length('d3')-8))

display(df_substr)

# COMMAND ----------

input = "/Date(1493596800000)/"
len(input)

# COMMAND ----------

# MAGIC %md
# MAGIC - Each row in data contains **3 string fields** like: "/Date(1493596800000)/"
# MAGIC - These strings are **timestamps in Unix Epoch milliseconds** format, **wrapped inside /Date(...)/**
# MAGIC       
# MAGIC        "/Date(1493596800000)/"` → actual timestamp is `1493596800000`
# MAGIC
# MAGIC - **substring('d1', 7, length('d1') - 8)**
# MAGIC   - substring **extracts a portion of a string** from a column.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC      "/Date(1493596800000)/"
# MAGIC
# MAGIC       INDEX of full string    =>    1   2   3   4   5   6   7   8   9   10   11   12    13   14   15   16   17   18   19   20   21
# MAGIC
# MAGIC                                     /   D   a   t   e   (   1   4   9    3    5    9     6    8    0    0    0    0    0    )    /
# MAGIC
# MAGIC       INDEX of full substring =>                            1   2   3    4    5    6     7    8    9   10   11   12   13
# MAGIC
# MAGIC - The **timestamp starts** at **position 7** right after **/Date(**
# MAGIC
# MAGIC - The closing **)/** is at the **end**.
# MAGIC - The **total length** of the **string is 21**.
# MAGIC - **length('d1') - 8 = 21-8 = 13** ensures we exclude **)/** from the end.
# MAGIC
# MAGIC **Why -8?**
# MAGIC - **length('d1')** gives **21** for **/Date(1493596800000)/**.
# MAGIC - We **start** extracting at **position 7**.
# MAGIC - The remaining portion to extract is **21 - 8 = 13** (which correctly gives **1493596800000**).

# COMMAND ----------

# DBTITLE 1,string, timestamp & custom
from pyspark.sql.functions import expr

df_epoch_wexpr = df\
.withColumn("dt1_default_str", from_unixtime(expr("cast(substring(d1, 7, length(d1)-8) as bigint)/1000"))) \
.withColumn("dt2_default_str", from_unixtime(expr("cast(substring(d2, 7, length(d2)-8) as bigint)/1000"))) \
.withColumn("dt3_default_str", from_unixtime(expr("cast(substring(d3, 7, length(d3)-8) as bigint)/1000"))) \
.withColumn("dt1_default_timestamp", from_unixtime(expr("cast(substring(d1, 7, length(d1)-8) as bigint)/1000")).cast("timestamp")) \
.withColumn("dt2_default_timestamp", from_unixtime(expr("cast(substring(d2, 7, length(d2)-8) as bigint)/1000")).cast("timestamp")) \
.withColumn("dt3_default_timestamp", from_unixtime(expr("cast(substring(d3, 7, length(d3)-8) as bigint)/1000")).cast("timestamp")) \
.withColumn("dt1_cust_str", from_unixtime(expr("cast(substring(d1, 7, length(d1)-8) as bigint) / 1000"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
.withColumn("dt2_cust_str", from_unixtime(expr("cast(substring(d2, 7, length(d2)-8) as bigint) / 1000"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
.withColumn("dt3_cust_str", from_unixtime(expr("cast(substring(d3, 7, length(d3)-8) as bigint) / 1000"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

display(df_epoch_wexpr)

# COMMAND ----------

# MAGIC %md
# MAGIC - Converting **Unix Time** to a **Human-Readable Format** of timestamp.
# MAGIC
# MAGIC       +----------+-------------------+
# MAGIC       |unix_time |timestamp          |
# MAGIC       +----------+-------------------+
# MAGIC       |1648974310|2023-04-03 09:45:10|
# MAGIC       +----------+-------------------+
# MAGIC
# MAGIC - **Returns:** string of **default: yyyy-MM-dd HH:mm:ss**

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, substring, length, col

df_epoch_woexpr = df \
.withColumn("dt1_default_str", from_unixtime((substring("d1", 7, length("d1") - 8).cast("bigint") / 1000))) \
.withColumn("dt2_default_str", from_unixtime((substring("d2", 7, length("d2") - 8).cast("bigint") / 1000))) \
.withColumn("dt3_default_str", from_unixtime((substring("d3", 7, length("d3") - 8).cast("bigint") / 1000))) \
.withColumn("dt1_default_timestamp", from_unixtime((substring("d1", 7, length("d1") - 8).cast("bigint") / 1000)).cast("timestamp")) \
.withColumn("dt2_default_timestamp", from_unixtime((substring("d2", 7, length("d2") - 8).cast("bigint") / 1000)).cast("timestamp")) \
.withColumn("dt3_default_timestamp", from_unixtime((substring("d3", 7, length("d3") - 8).cast("bigint") / 1000)).cast("timestamp")) \
.withColumn("dt1_cust_str", from_unixtime((substring("d1", 7, length("d1") - 8).cast("bigint") / 1000), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
.withColumn("dt2_cust_str", from_unixtime((substring("d2", 7, length("d2") - 8).cast("bigint") / 1000), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
.withColumn("dt3_cust_str", from_unixtime((substring("d3", 7, length("d3") - 8).cast("bigint") / 1000), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

display(df_epoch_woexpr)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 02**
# MAGIC - Using **timestamp_millis + substring + cast**

# COMMAND ----------

from pyspark.sql.functions import substring, length, col, timestamp_millis

df_epoch_millis = df \
.withColumn("millis_d1", substring("d1", 7, length("d1") - 8).cast("bigint")) \
.withColumn("millis_d2", substring("d2", 7, length("d2") - 8).cast("bigint")) \
.withColumn("millis_d3", substring("d3", 7, length("d3") - 8).cast("bigint")) \
.withColumn("dt1_default_timestamp", timestamp_millis(col("millis_d1"))) \
.withColumn("dt2_default_timestamp", timestamp_millis(col("millis_d2"))) \
.withColumn("dt3_default_timestamp", timestamp_millis(col("millis_d3"))) \
.withColumn("dt1_cust_str", timestamp_millis(col("millis_d1")).cast("string")) \
.withColumn("dt2_cust_str", timestamp_millis(col("millis_d2")).cast("string")) \
.withColumn("dt3_cust_str", timestamp_millis(col("millis_d3")).cast("string"))

display(df_epoch_millis)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 03**
# MAGIC - Using **regexp_extract + from_unixtime**

# COMMAND ----------

# Extract the numeric timestamp from the string and convert to timestamp
df2 = df\
.withColumn("d1", from_unixtime(regexp_extract(col("d1"), r"(\d+)", 1).cast("bigint")/1000).cast("timestamp")) \
.withColumn("d2", from_unixtime(regexp_extract(col("d2"), r"(\d+)", 1).cast("bigint")/1000).cast("timestamp")) \
.withColumn("d3", from_unixtime(regexp_extract(col("d3"), r"(\d+)", 1).cast("bigint")/1000).cast("timestamp"))

# Display the transformed DataFrame
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC - **regexp_extract(col("from_date"), r"(\d+)", 1)** => Extracts the **numeric** part from the **string**.
# MAGIC
# MAGIC - **.cast("bigint")** => Converts it into a **long** integer.
# MAGIC
# MAGIC - **/ 1000** => Converts **milliseconds to seconds**.
# MAGIC
# MAGIC - **from_unixtime(...).cast("timestamp")** => Converts it into a proper **timestamp**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 4**
# MAGIC - Using **regexp_replace + cast**.
# MAGIC - Replaces **all non-numeric characters** using **regexp_replace**.

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

df4 = df\
.withColumn("d1", from_unixtime(regexp_replace(col("d1"), "[^0-9]", "").cast("bigint")/1000).cast("timestamp")) \
.withColumn("d2", from_unixtime(regexp_replace(col("d2"), "[^0-9]", "").cast("bigint")/1000).cast("timestamp")) \
.withColumn("d3", from_unixtime(regexp_replace(col("d3"), "[^0-9]", "").cast("bigint")/1000).cast("timestamp"))

display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 5**
# MAGIC - Using **translate + cast**
# MAGIC - Uses **translate()** to **remove unwanted characters** efficiently.

# COMMAND ----------

from pyspark.sql.functions import translate

df3 = df\
.withColumn("d1", from_unixtime(translate(col("d1"), "/Date()", "").cast("bigint") / 1000).cast("timestamp")) \
.withColumn("d2", from_unixtime(translate(col("d2"), "/Date()", "").cast("bigint") / 1000).cast("timestamp")) \
.withColumn("d3", from_unixtime(translate(col("d3"), "/Date()", "").cast("bigint") / 1000).cast("timestamp"))

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 6**
# MAGIC - Using **UDF (User-Defined Function)**

# COMMAND ----------

date_str = "/Date(1493596800000)/"
millis = int(date_str.strip("/Date()/"))
display(millis)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType
import datetime

def convert_to_timestamp(date_str):
    millis = int(date_str.strip("/Date()/"))
    return datetime.datetime.fromtimestamp(millis / 1000)

convert_udf = udf(convert_to_timestamp, TimestampType())

df5 = df.withColumn("d1", convert_udf(col("d1"))) \
        .withColumn("d2", convert_udf(col("d2"))) \
        .withColumn("d3", convert_udf(col("d3")))

display(df5)