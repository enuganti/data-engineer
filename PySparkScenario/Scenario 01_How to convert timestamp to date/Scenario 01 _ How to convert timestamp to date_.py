# Databricks notebook source
# MAGIC %md
# MAGIC #### How to convert timestamp to date?

# COMMAND ----------

# MAGIC %md
# MAGIC |             source                |          bronze                  |     silver      |     gold       |
# MAGIC |-----------------------------------|----------------------------------|-----------------|----------------|
# MAGIC |  2025-08-25T00:00:00.000+00:00    |   2025-08-25T00:00:00.000+00:00  |    2025-09-14   |   2025-09-14   |

# COMMAND ----------

from pyspark.sql.functions import to_date, col

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 01**

# COMMAND ----------

# DBTITLE 1,Bronze
df_ts = spark.read.csv("/Volumes/@azureadb/pyspark/timestamp/timestamptodate.csv", inferSchema=True, header=True)
display(df_ts.limit(10))

# COMMAND ----------

# Convert string column to date type
df_with_date = df_ts.withColumn("date_parsed", to_date(col("start_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
display(df_with_date.limit(15))

# COMMAND ----------

# DBTITLE 1,Silver
# Convert string column to date type
df_silver = df_ts.withColumn("start_date", to_date(col("start_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
display(df_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Format explanation:
# MAGIC |      Format      |     description   |
# MAGIC |------------------|-------------------|
# MAGIC |  **yyyy-MM-dd**  | year, month, day  |
# MAGIC |  **'T'**         | literal T in the string |
# MAGIC |  **HH:mm:ss**    | hour, minute, second  |
# MAGIC |  **.SSS**        | milliseconds  |
# MAGIC |  **XXX**         | timezone offset (+00:00) |

# COMMAND ----------

# MAGIC %md
# MAGIC #### to_date()
# MAGIC
# MAGIC ✅ **to_date()** function is used to format a **"date string" (or) "timestamp string" column** into the **"Date" Type column** using a **specified format**.
# MAGIC
# MAGIC ✅ If the **format is not provided**, to_date() takes the **default value as 'yyyy-MM-dd'**.
# MAGIC
# MAGIC ✅ Extracts only the **date** portion **(removes time part if present)**.
# MAGIC
# MAGIC ✅ Returns **NULL** if the format does **not match**.
# MAGIC
# MAGIC - **to_date():** extracts **only the date** part (ignores time).
# MAGIC - **to_timestamp():** parses **both date and time**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Syntax:
# MAGIC
# MAGIC      to_date(col, format=None)
# MAGIC
# MAGIC **Parameters:**
# MAGIC
# MAGIC - **col** → Column name or expression containing the **date string/timestamp**.
# MAGIC
# MAGIC - **format (optional)** → A string specifying the format of the input date (using Java SimpleDateFormat patterns). If **not provided**, it tries to parse with the **default format yyyy-MM-dd**.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

convert = convert_timestamp_date(df_ts, "start_date", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
display(convert.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **Casts multiple columns of a DataFrame to given types?**

# COMMAND ----------

import pyspark.sql.functions as F

# Sample data
data = [
    ("Albert", "25", "55000.50", "true", "2025-08-25", "2025-08-25T15:30:00.000+00:00"),
    ("Baskar", "30", "72000.00", "false", "2024-12-31", "2024-12-31T08:45:15.000+00:00"),
    ("Chetan", "27", "63000.75", "true", "2023-01-15", "2023-01-15T20:00:00.000+00:00"),
    ("Dravid", "26", "66000.50", "true", "2024-06-20", "2024-05-22T15:40:55.000+00:00"),
    ("Nishant", "32", "98700.00", "false", "2023-10-21", "2020-10-31T06:45:45.000+00:00"),
    ("David", "29", "34512.75", "true", "2023-03-15", "2021-08-28T20:15:55.000+00:00"),
    ("Mohan", "33", "34908.50", "true", "2022-04-15", "2019-09-29T19:35:55.000+00:00"),
    ("Niroop", "35", "49654.98", "false", "2021-02-18", "2024-05-22T08:45:25.000+00:00"),
    ("Pushpa", "23", "44111.99", "true", "2020-07-19", "2023-09-25T20:00:00.000+00:00")
]

# Define schema
columns = ["name", "age", "salary", "is_active", "join_date", "last_login"]

df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

col_type_map = {
    "age": "int",
    "salary": "double",
    "is_active": "boolean",
    "join_date": "date",
    "last_login": "timestamp"
}

# COMMAND ----------

# Apply casting with correct timestamp format
df_casted = cast_dataframe_columns(df, col_type_map, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
display(df_casted)