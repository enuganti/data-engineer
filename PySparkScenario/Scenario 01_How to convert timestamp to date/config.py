# Databricks notebook source
from pyspark.sql.functions import to_date, col

# COMMAND ----------

def convert_timestamp_date(df, col_name, date_format=None):
    return df.withColumn(col_name, to_date(col(col_name), date_format))

# COMMAND ----------

def cast_dataframe_columns(df, col_type_map, date_format=None):
    for col_name, target_type in col_type_map.items():
        target_type = target_type.lower()

        if target_type == "string":
            df = df.withColumn(col_name, F.col(col_name).cast("string"))

        elif target_type in ["int", "integer"]:
            df = df.withColumn(col_name, F.col(col_name).cast("int"))

        elif target_type in ["double", "float"]:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

        elif target_type == "boolean":
            df = df.withColumn(col_name, F.col(col_name).cast("boolean"))

        elif target_type == "date":
            df = df.withColumn(col_name, F.to_date(F.col(col_name)))

        elif target_type == "timestamp":
            df = df.withColumn(col_name, F.to_date(F.col(col_name), date_format))
    return df