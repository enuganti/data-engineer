# Databricks notebook source
# MAGIC %md
# MAGIC ##### How to Cast multiple columns of a DataFrame to given types?
# MAGIC
# MAGIC - why filtering with `if date_format`?

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) with `if date_format`:

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, DoubleType

# COMMAND ----------

# All columns are in string type
# All rows of timestamp are in default format

# Sample data
data = [
    ("Albert", "25", "6.5", "2025-08-25T15:30:00.000+00:00"),
    ("Baskar", "30", "1.5", "2024-12-31T08:45:15.000+00:00"),
    ("Chetan", "27", "2.7", "2023-01-15T20:00:00.000+00:00"),
    ("Dravid", "26", "4.9", "2024-05-22T15:40:55.000+00:00"),
    ("Nishant", "32", "8.2", "2020-10-31T06:45:45.000+00:00"),
    ("David", "29", "5.1", "2021-08-28T20:15:55.000+00:00"),
    ("Mohan", "33", "9.3", "2019-09-29T19:35:55.000+00:00"),
    ("Niroop", "35", "6.7", "2024-05-22T08:45:25.000+00:00"),
    ("Pushpa", "23", "4.0", "2023-09-25T20:00:00.000+00:00")
]

# Define schema
columns = ["name", "age", "distance", "last_login"]

df_frmt = spark.createDataFrame(data, columns)
display(df_frmt)

# COMMAND ----------

col_type_map_none = {
    "name": "string",
    "age": "int",
    "last_login": "timestamp"
}

# COMMAND ----------

def convert_columns_date_format(df, col_name, date_format=None):
    """
    How to Cast multiple columns of a DataFrame to given types?

    Args:
        df (DataFrame): Input DataFrame
        col_type_map (dict): Dictionary of { "column_name": "target_dataType" }
                             target_dataType can be: string, int, double, boolean, date, timestamp
        date_format (str, optional): Custom format for date/timestamp parsing

    Returns:
        DataFrame: Transformed DataFrame with casted columns
    """
    for column_name, target_dataType in col_type_map_none.items():
        target_dataType = target_dataType.lower()

        if target_dataType == "timestamp":
            if date_format:
                df = df.withColumn(column_name, F.to_timestamp(F.col(column_name), date_format))
    return df

# COMMAND ----------

# Convert last_login column to timestamp with default format

df_conv_default = convert_columns_date_format(df_frmt, "last_login", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
display(df_conv_default)

# COMMAND ----------

# skip date_format, last_login column is in default format
df_conv_none = convert_columns_date_format(df_frmt, "last_login")
display(df_conv_none)

# COMMAND ----------

# MAGIC %md
# MAGIC - The function only converts to **timestamp** if a **date_format** is provided.
# MAGIC - Without **date_format**, the conversion is **skipped** in the function logic.

# COMMAND ----------

# MAGIC %md
# MAGIC      if date_format:
# MAGIC          df = df.withColumn(column_name, F.to_timestamp(F.col(column_name), date_format))
# MAGIC
# MAGIC - **if date_format:** checks if the user has **passed a value** for **date_format**.
# MAGIC - In Python, below are treated as **Falsey**, so the **block only runs** if you provided a real string like **"yyyy-MM-dd"**.
# MAGIC   - **None**
# MAGIC   - **"" (empty string)**
# MAGIC   - **False** 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) without `if date_format`:

# COMMAND ----------

def convert_columns_wo_date_format(df, col_name, date_format=None):
    for column_name, target_dataType in col_type_map_none.items():
        target_dataType = target_dataType.lower()

        if target_dataType == "timestamp":
            df = df.withColumn(column_name, F.to_timestamp(F.col(column_name), date_format))
    return df

# COMMAND ----------

df_conv_wo_date_format = convert_columns_wo_date_format(df_frmt, "last_login")
display(df_conv_wo_date_format)


# COMMAND ----------

# MAGIC %md
# MAGIC - The column **'last_login'** is converted as **timestamp** because in **convert_columns_wo_date_format**, **F.to_date(F.col(column_name), date_format)** is called **regardless of whether date_format is None**.
# MAGIC - When **date_format is None**, **F.to_date** uses the default format **yyyy-MM-dd** and tries to parse the string.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) All data types

# COMMAND ----------

# Sample data
data = [
    ("Albert", "25", "1500.55", "6.5", "2025-08-25T15:30:00.000+00:00"),
    ("Baskar", "30", "999.99", "1.5", "2024-12-31T08:45:15.000+00:00"),
    ("Chetan", "27", "1503.55", "2.7", "2023-01-15T20:00:00.000+00:00"),
    ("Dravid", "26", "998.99", "4.9", "2024-05-22T15:40:55.000+00:00"),
    ("Nishant", "32", "789.87", "8.2", "2020-10-31T06:45:45.000+00:00"),
    ("David", "29", "159.09", "5.1", "2021-08-28T20:15:55.000+00:00"),
    ("Mohan", "33", "346.23", "9.3", "2019-09-29T19:35:55.000+00:00"),
    ("Niroop", "35", "123.00", "6.7", "2024-05-22T08:45:25.000+00:00"),
    ("Pushpa", "23", "698.11", "4.0", "2023-09-25T20:00:00.000+00:00")
]

# Define schema
columns = ["name", "age", "amount", "distance", "last_login"]

df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

def convert_columns_date_format_all(df, col_type_map, date_format=None):
    for col_name, target_type in col_type_map.items():
        target_type = target_type.lower()

        if target_type == "timestamp":
            if date_format:
                df = df.withColumn(col_name, F.to_timestamp(F.col(col_name), date_format))
            else:
                df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))
        elif target_type == "int":
            df = df.withColumn(col_name, F.col(col_name).cast(LongType()))
        elif target_type == "double":
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
        # elif target_type == "string":
        #     df = df.withColumn(col_name, F.col(col_name).cast("string"))
        else:
            pass
    return df

# COMMAND ----------

col_type_map = {
    "name": "string",
    "age": "int",
    "amount": "double",
    "distance": "double",
    "last_login": "timestamp"
}

# COMMAND ----------

df_convert_columns_date_format_all = convert_columns_date_format_all(df, col_type_map, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
display(df_convert_columns_date_format_all)