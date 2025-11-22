# Databricks notebook source
# MAGIC %md
# MAGIC ##### Why passing argument `date_format=None` in function?
# MAGIC - How to **cast** multiple columns to specific data types?
# MAGIC - How the table looks before and after casting?
# MAGIC - when the type is **"date" or "timestamp"**, you can **optionally** pass a **date_format**.
# MAGIC - so that Spark knows how to parse the **string** values into proper **DateType or TimestampType**.

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,date cols: default format
# All columns are in string type
# All rows of date & timestamp are in default format

# Sample data
data = [
    ("Albert", "25", "55000.50", "true", "2025-08-25", "6", "2025-08-25T15:30:00.000+00:00"),      # All rows of date & timestamp are in default format
    ("Baskar", "30", "72000.00", "false", "2024-12-31", "7", "2024-12-31T08:45:15.000+00:00"),
    ("Chetan", "27", "63000.75", "true", "2023-01-15", "8", "2023-01-15T20:00:00.000+00:00"),
    ("Dravid", "26", "66000.50", "true", "2024-06-20", "9", "2024-05-22T15:40:55.000+00:00"),
    ("Nishant", "32", "98700.00", "false", "2023-10-21", "10", "2020-10-31T06:45:45.000+00:00"),
    ("David", "29", "34512.75", "true", "2023-03-15", "4", "2021-08-28T20:15:55.000+00:00"),
    ("Mohan", "33", "34908.50", "true", "2022-04-15", "12", "2019-09-29T19:35:55.000+00:00"),
    ("Niroop", "35", "49654.98", "false", "2021-02-18", "3", "2024-05-22T08:45:25.000+00:00"),
    ("Pushpa", "23", "44111.99", "true", "2020-07-19", "5", "2023-09-25T20:00:00.000+00:00")
]

# Define schema
columns = ["name", "age", "salary", "is_active", "join_date", "Exp" ,"last_login"]

df_default_format = spark.createDataFrame(data, columns)
display(df_default_format)

# COMMAND ----------

col_type_map = {
    "age": "int",
    "salary": "double",
    "is_active": "boolean",
    "join_date": "date",
    "Exp": "Int",
    "last_login": "timestamp"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Scenario 01
# MAGIC - **date_format=None**
# MAGIC - **Default argument behavior**

# COMMAND ----------

def cast_dataframe_columns_wo(df, col_type_map, date_format=None):
    for col_name, data_type in col_type_map.items():
        data_type = data_type.lower()

        if data_type == "string":
            df = df.withColumn(col_name, F.col(col_name).cast("string"))

        elif data_type in ["int", "Int", "integer"]:
            df = df.withColumn(col_name, F.col(col_name).cast("int"))

        elif data_type in ["double", "float"]:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

        elif data_type == "boolean":
            df = df.withColumn(col_name, F.col(col_name).cast("boolean"))

        elif data_type == "date":
            df = df.withColumn(col_name, F.to_date(F.col(col_name)))              # since date & timestamp are in default format, no need to pass date_format

        elif data_type == "timestamp":
            df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))         # since date & timestamp are in default format, no need to pass date_format
    return df

# COMMAND ----------

# DBTITLE 1,default_format
# Apply casting with correct date / timestamp format
# df_default_format: date & timestamp are in default format
# since date & timestamp are in default format, no need to pass date_format -> cast_dataframe_columns_wo(df, col_type_map, date_format=None)

df_casted_wo = cast_dataframe_columns_wo(df_default_format, col_type_map)
display(df_casted_wo)

# COMMAND ----------

# MAGIC %md
# MAGIC | Output schema BEFORE casting |   Output schema AFTER casting |
# MAGIC |------------------------------|-------------------------------|
# MAGIC |         `name`:string        |         `name`:string         |  
# MAGIC |         `age`:`string`       |         `age`:`integer`       |
# MAGIC |        `salary`:`string`     |        `salary`:`double`      |
# MAGIC |       `is_active`:`string`   |      `is_active`:`boolean`    |
# MAGIC |       `join_date`:`string`   |        `join_date`:`date`     |
# MAGIC |         `Exp`:`string`       |       `Exp`:`integer`         |
# MAGIC |       `last_login`:`string`  |     `last_login`:`timestamp`  |    

# COMMAND ----------

# DBTITLE 1,custom_format
# Apply casting with correct date / timestamp format
# df_cust_format: date & timestamp are in custom format
# since date & timestamp are in custom format, pass date_format -> cast_dataframe_columns_wo(df, col_type_map, date_format="%m-%d-%Y")
# date_format="%m-%d-%Y" -> month-day-year
# date_format="%m-%d-%Y %H:%M:%S.%f%z" -> month-day-year hour:minute:second.microsecond
# since date_format not passed getting error -> [CAST_INVALID_INPUT] The value '08-25-2025' of the type "STRING" cannot be cast to "DATE" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018

df_casted_cust = cast_dataframe_columns_wo(df_cust_format, col_type_map)
display(df_casted_cust)

# COMMAND ----------

# MAGIC %md
# MAGIC **date_format** has a **default value** of **None**.
# MAGIC
# MAGIC       def cast_dataframe_columns_wo(df, col_type_map, date_format=None):
# MAGIC
# MAGIC - If the user **does not** provide a **date_format**, the function **still works**, it will just treat **date_format** as **None**.
# MAGIC    - Spark uses its **default date** `parsing rules`, which expect the input to be in the **standard format**  
# MAGIC
# MAGIC          # No format passed → date_format is None
# MAGIC          cast_dataframe_columns_wo(df, {"join_date": "date"})
# MAGIC
# MAGIC          -> yyyy-MM-dd (for date)
# MAGIC          -> yyyy-MM-dd HH:mm:ss (for timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Scenario 02
# MAGIC - date = "MM-dd-yyyy" -> custom format
# MAGIC - date_format = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

# COMMAND ----------

# DBTITLE 1,date cols: custom format
# All columns are in string type
# All rows of date & timestamp are in custom format

# Sample data
data = [
    ("Albert", "25", "55000.50", "true", "08-25-2025", "6", "08-25-2025T15:30:00.000+00:00"),     # All rows of date & timestamp are in default format
    ("Baskar", "30", "72000.00", "false", "12-30-2024", "7", "12-31-2024T08:45:15.000+00:00"),
    ("Chetan", "27", "63000.75", "true", "01-15-2023", "8", "01-15-2023T20:00:00.000+00:00"),
    ("Dravid", "26", "66000.50", "true", "06-20-2024", "9", "05-22-2024T15:40:55.000+00:00"),
    ("Nishant", "32", "98700.00", "false", "10-21-2023", "10", "10-31-2020T06:45:45.000+00:00"),
    ("David", "29", "34512.75", "true", "03-15-2023", "4", "08-28-2021T20:15:55.000+00:00"),
    ("Mohan", "33", "34908.50", "true", "04-15-2022", "12", "09-29-2019T19:35:55.000+00:00"),
    ("Niroop", "35", "49654.98", "false", "02-18-2021", "3", "05-22-2024T08:45:25.000+00:00"),
    ("Pushpa", "23", "44111.99", "true", "07-19-2020", "5", "09-25-2023T20:00:00.000+00:00")
]

# Define schema
columns = ["name", "age", "salary", "is_active", "join_date", "Exp" ,"last_login"]

df_cust_format = spark.createDataFrame(data, columns)
display(df_cust_format)

# COMMAND ----------

def cast_dataframe_columns_tformat(df, col_type_map, date_format=None):
    for col_name, data_type in col_type_map.items():
        data_type = data_type.lower()

        if data_type == "string":
            df = df.withColumn(col_name, F.col(col_name).cast("string"))

        elif data_type in ["int", "Int", "integer"]:
            df = df.withColumn(col_name, F.col(col_name).cast("int"))

        elif data_type in ["double", "float"]:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

        elif data_type == "boolean":
            df = df.withColumn(col_name, F.col(col_name).cast("boolean"))

        elif data_type == "date":
            # Handle custom date format "08-25-2025" -> "MM-dd-yyyy"
            df = df.withColumn(col_name, F.to_date(F.col(col_name), "MM-dd-yyyy"))

        elif data_type == "timestamp":
            df = df.withColumn(col_name, F.to_timestamp(F.col(col_name), date_format))
    return df

# COMMAND ----------

# Apply casting with correct date / timestamp format
df_casted_tformat = cast_dataframe_columns_tformat(df_cust_format, col_type_map, "MM-dd-yyyy'T'HH:mm:ss.SSSXXX")
display(df_casted_tformat)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Scenario 03
# MAGIC - date_format_map=**None**

# COMMAND ----------

def cast_dataframe_columns_map_wo(df, col_type_map, date_format_map=None):
    for col_name, data_type in col_type_map.items():
        data_type = data_type.lower()

        if data_type == "string":
            df = df.withColumn(col_name, F.col(col_name).cast("string"))

        elif data_type in ["int", "Int", "integer"]:
            df = df.withColumn(col_name, F.col(col_name).cast("int"))

        elif data_type in ["double", "float"]:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

        elif data_type == "boolean":
            df = df.withColumn(col_name, F.col(col_name).cast("boolean"))

        elif data_type == "date":
            df = df.withColumn(col_name, F.to_date(F.col(col_name)))

        elif data_type == "timestamp":
            df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))
    return df

# COMMAND ----------

df_casted_map_wo = cast_dataframe_columns_map_wo(df_default_format, col_type_map)
display(df_casted_map_wo)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Scenario 04
# MAGIC - date_format_map=**None**
# MAGIC - date_format_map = **{"date": "MM-dd-yyyy", "timestamp": "MM-dd-yyyy'T'HH:mm:ss.SSSXXX"}**

# COMMAND ----------

def cast_dataframe_columns_map(df, col_type_map, date_format_map=None):
    for col_name, data_type in col_type_map.items():
        data_type = data_type.lower()

        if data_type == "string":
            df = df.withColumn(col_name, F.col(col_name).cast("string"))

        elif data_type in ["int", "Int", "integer"]:
            df = df.withColumn(col_name, F.col(col_name).cast("int"))

        elif data_type in ["double", "float"]:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

        elif data_type == "boolean":
            df = df.withColumn(col_name, F.col(col_name).cast("boolean"))

        elif data_type == "date":
            df = df.withColumn(col_name, F.to_date(F.col(col_name), date_format_map.get("date")))

        elif data_type == "timestamp":
            df = df.withColumn(col_name, F.to_timestamp(F.col(col_name), date_format_map.get("timestamp")))
    return df

# COMMAND ----------

df_casted_cfrmt = cast_dataframe_columns_map(
    df_cust_format,
    col_type_map,
    {"date": "MM-dd-yyyy", "timestamp": "MM-dd-yyyy'T'HH:mm:ss.SSSXXX"}
)
display(df_casted_cfrmt)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Scenario 05
# MAGIC - date_format_map=**None**
# MAGIC - date_format_map = **{"my_date": "yyyy-MM-dd", "my_timestamp": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"}**

# COMMAND ----------

def cast_dataframe_columns_dtformat(df, col_type_map, date_format_map=None):
    for col_name, target_type in col_type_map.items():
        target_type = target_type.lower()
        fmt = None if date_format_map is None else date_format_map.get(col_name)

        if target_type == "string":
            df = df.withColumn(col_name, F.col(col_name).cast("string"))

        elif target_type in ["int", "integer"]:
            df = df.withColumn(col_name, F.col(col_name).cast("int"))

        elif target_type in ["double", "float"]:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

        elif target_type == "boolean":
            df = df.withColumn(col_name, F.col(col_name).cast("boolean"))

        elif target_type == "date":
            if fmt:
                df = df.withColumn(col_name, F.to_date(F.col(col_name), fmt))
            else:
                df = df.withColumn(col_name, F.to_date(F.col(col_name)))

        # For timestamp, apply both to_date and to_timestamp as new columns
        if target_type == "timestamp":
            if fmt:
                df = df.withColumn(f"{col_name}_date", F.to_date(F.col(col_name), fmt))
                df = df.withColumn(f"{col_name}_ts", F.to_timestamp(F.col(col_name), fmt))
            else:
                df = df.withColumn(f"{col_name}_date", F.to_date(F.col(col_name)))
                df = df.withColumn(f"{col_name}_ts", F.to_timestamp(F.col(col_name)))
    return df

# COMMAND ----------

date_format_map = {
    "my_date": "yyyy-MM-dd",
    "my_timestamp": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
}

# COMMAND ----------

# Apply casting with correct timestamp format
df_casted_dtformat = cast_dataframe_columns_dtformat(df_default_format, col_type_map, date_format_map)
display(df_casted_dtformat)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### How Spark handles it?

# COMMAND ----------

# MAGIC %md
# MAGIC If you call:
# MAGIC
# MAGIC         F.to_date(F.col("start_date"))
# MAGIC - Spark uses its **default parser (yyyy-MM-dd)**.
# MAGIC
# MAGIC If you call:
# MAGIC
# MAGIC       F.to_date(F.col("start_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
# MAGIC
# MAGIC - Spark will use your **custom format**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Why set None?

# COMMAND ----------

# MAGIC %md
# MAGIC - **Without =None**, the argument becomes **mandatory**.
# MAGIC
# MAGIC       def convert_dataframe_columns(df, col_type_map, date_format):
# MAGIC           ...
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - Now you **must** always call:
# MAGIC
# MAGIC       convert_dataframe_columns(df, {"start_date": "date"}, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

# COMMAND ----------

# MAGIC %md
# MAGIC - If you **don’t** pass a **format**, Python will raise:
# MAGIC
# MAGIC       TypeError: missing 1 required positional argument: 'date_format'
# MAGIC
# MAGIC - By making it **None**, you can handle **two cases**:
# MAGIC   - User **provided a format** → use it.
# MAGIC   - User **didn’t provide one** → either fall back to default Spark behavior or skip formatting.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Scenario 06
# MAGIC - date_format
# MAGIC - date_format = **"yyyy-MM-dd'T'HH:mm:ss.SSSXXX"**

# COMMAND ----------

def cast_dataframe_columns_none(df, col_type_map, date_format):
    for col_name, data_type in col_type_map.items():
        data_type = data_type.lower()

        if data_type == "string":
            df = df.withColumn(col_name, F.col(col_name).cast("string"))

        elif data_type in ["int", "integer"]:
            df = df.withColumn(col_name, F.col(col_name).cast("int"))

        elif data_type in ["double", "float"]:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

        elif data_type == "boolean":
            df = df.withColumn(col_name, F.col(col_name).cast("boolean"))

        elif data_type == "date":
            df = df.withColumn(col_name, F.to_date(F.col(col_name)))

        elif data_type == "timestamp":
            df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))
    return df

# COMMAND ----------

# Apply casting with correct timestamp format
df_casted_none = cast_dataframe_columns_none(df_default_format, col_type_map)
display(df_casted_none)

# COMMAND ----------

# Apply casting with correct timestamp format
df_casted_format_none = cast_dataframe_columns_none(df_default_format, col_type_map, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
display(df_casted_format_none)

# COMMAND ----------

def cast_dataframe_columns_none_cust(df, col_type_map, date_format_map):
    for col_name, data_type in col_type_map.items():
        data_type = data_type.lower()

        if data_type == "string":
            df = df.withColumn(col_name, F.col(col_name).cast("string"))

        elif data_type in ["int", "integer"]:
            df = df.withColumn(col_name, F.col(col_name).cast("int"))

        elif data_type in ["double", "float"]:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

        elif data_type == "boolean":
            df = df.withColumn(col_name, F.col(col_name).cast("boolean"))

        elif data_type == "date":
            df = df.withColumn(col_name, F.to_date(F.col(col_name), date_format_map.get("date")))

        elif data_type == "timestamp":
            df = df.withColumn(col_name, F.to_timestamp(F.col(col_name), date_format_map.get("timestamp")))
    return df

# COMMAND ----------

df_casted_cfrmt_none = cast_dataframe_columns_none_cust(
    df_cust_format,
    col_type_map,
    {"date": "MM-dd-yyyy", "timestamp": "MM-dd-yyyy'T'HH:mm:ss.SSSXXX"}
)
display(df_casted_cfrmt_none)