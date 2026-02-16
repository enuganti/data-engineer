# Databricks notebook source
# MAGIC %md
# MAGIC #### How to iterate columns of a dataframe using List Comprehension?

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

# DBTITLE 1,import required functions
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Read Sample dataset
df = spark.read.csv("/FileStore/tables/iterate_columns.csv", header=True, inferSchema=True).toPandas()
df.head(10)

# COMMAND ----------

print("Names of Columns:\n", df.columns)
print("\nNumber of Columns:", len(df.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC **How to iterate columns of a dataframe using List Comprehension?**

# COMMAND ----------

# List all column names in the DataFrame
[col for col in df.columns]

# COMMAND ----------

# List of columns to drop from the DataFrame
drop_cols = ["Department", "Series_reference15", "Data_value17", "Series_reference0", "Data_value2", "line_code", "Series_title_5"]

# Create a new list of columns excluding the ones in drop_cols
columns = [col for col in df.columns if col not in drop_cols]

# Display the final list of columns after dropping specified columns
columns

# COMMAND ----------

# MAGIC %md
# MAGIC |  S.No  |       df.columns          |         drop_columns            |
# MAGIC |--------|---------------------------|---------------------------------|                    
# MAGIC |   1    |   Series_reference0       |        Series_reference0        |
# MAGIC |   2    |       Period1             |                                 |
# MAGIC |   3    |     Data_value2           |           Data_value2           |
# MAGIC |   4    |       STATUS3             |                                 |
# MAGIC |   5    |       UNITS4              |                                 |
# MAGIC |   6    |      Service              |                                 |
# MAGIC |   7    |    Department             |           Department            |
# MAGIC |   8    |   description             |                                 |
# MAGIC |   9    |    industry               |                                 |
# MAGIC |  10    |     level                 |                                 |
# MAGIC |  11    |     size                  |                                 |
# MAGIC |  12    |   line_code               |           line_code             |
# MAGIC |  13    |     value                 |                                 |
# MAGIC |  14    |   Business                |                                 |
# MAGIC |  15    |   Footnotes               |                                 |
# MAGIC |  16    | Series_reference15        |      Series_reference15         |
# MAGIC |  17    |   Period16                |                                 |
# MAGIC |  18    |   Data_value17            |         Data_value17            |
# MAGIC |  19    |   STATUS18                |                                 |
# MAGIC |  20    |   UNITS19                 |                                 | 
# MAGIC |  21    |   MAGNTUDE                |                                 |
# MAGIC |  22    |   Subject                 |                                 |
# MAGIC |  23    |   Group                   |                                 |
# MAGIC |  24    |   Series_title_1          |                                 |
# MAGIC |  25    |   Series_title_2          |                                 |
# MAGIC |  26    |   Series_title_3          |                                 |
# MAGIC |  27    |   Series_title_4          |                                 |
# MAGIC |  28    |   Series_title_5          |       Series_title_5            |

# COMMAND ----------

print("Number of Columns:", len(df.columns))
print("Latest Number of Columns:", len(columns))