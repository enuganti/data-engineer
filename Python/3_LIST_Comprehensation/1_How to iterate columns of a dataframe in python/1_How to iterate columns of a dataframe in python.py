# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/iterate_columns.csv", header=True, inferSchema=True).toPandas()
df.head(10)

# COMMAND ----------

print("Names of Columns:\n", df.columns)
print("\nNumber of Columns:", len(df.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 01: How to iterate columns using Loop**

# COMMAND ----------

[col for col in df.columns]

# COMMAND ----------

drop_cols = ["Department", "Series_reference15", "Data_value17", "Series_reference0", "Data_value2", "line_code", "Series_title_5"]

columns = [col for col in df.columns if col not in drop_cols]
columns

# COMMAND ----------

print("Number of Columns:", len(df.columns))
print("Latest Number of Columns:", len(columns))