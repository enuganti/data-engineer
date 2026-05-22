# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Create Empty DataFrame without Schema (no columns)**

# COMMAND ----------

# Create empty DatFrame with no schema (no columns)
df3 = spark.createDataFrame([], StructType([]))
display(df3)
df3.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Create Empty DataFrame with Schema**

# COMMAND ----------

# Create Schema
schema = StructType([
  StructField('FirstName', StringType(), True),
  StructField('Age', IntegerType(), True),
  StructField('Experience', IntegerType(), True),
  StructField('Label_Type', StringType(), True),
  StructField('Last_transaction_date', StringType(), True),
  StructField('last_timestamp', StringType(), True),
  StructField('Sensex_Category', StringType(), True)
  ])

# COMMAND ----------

# Create empty DataFrame directly.
emp_df = spark.createDataFrame([], schema)
display(emp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Creating empty DataFrame with NULL placeholders**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Sales", IntegerType(), True),
])

# Just placeholders with all nulls
empty_data = [(None, None, None, None),
              (None, None, None, None),
              (None, None, None, None),
              (None, None, None, None),
              (None, None, None, None)]
              
df_null_placeholder = spark.createDataFrame(empty_data, schema)
display(df_null_placeholder)

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Sales", IntegerType(), True),
])

# Just placeholders with all nulls
empty_data = [(1, None, None, None),
              (2, "Neol", 25, 150),
              (3, None, None, None),
              (4, "Niroop", 35, 350),
              (5, None, None, None)]
              
df_null_placeholder = spark.createDataFrame(empty_data, schema)
display(df_null_placeholder)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) How to union existing dataframe to empty dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC **⚠️ Things to Avoid**
# MAGIC
# MAGIC - **Different schemas:** Will throw an error.
# MAGIC - **Mismatched column names or types:** Columns must match exactly in name, type, and order.

# COMMAND ----------

# DBTITLE 1,empty dataframe
empty_df = spark.createDataFrame([], emp_df.schema)
display(empty_df)

# COMMAND ----------

# DBTITLE 1,existing dataframe
# Sample DataFrame to union
data = [("Kalmesh", 15, 9, "Medium", "23/11/2025", "2025-09-27T19:45:35", "Admin"),
        ("Rohan", 18, 5, "Small", "20/12/2024", "2023-11-29T19:55:35", "Sales"),
        ("Kiran", 19, 3, "Average", "15/06/2023", "2024-09-27T19:35:35", "Marketing"),
        ("Asha", 29, 8, "Short", "13/03/2021", "2021-09-27T19:25:35", "IT"),
        ("Amir", 32, 6, "Long", "17/09/2020", "2018-05-21T15:49:39", "Maintenance"),
        ("Rupesh", 36, 11, "Less", "19/02/2022", "2016-06-27T19:45:35", "Logistics"),
        ("Krupa", 50, 7, "Medium", "12/06/2018", "2019-08-17T22:25:45", "Supplychain"),
        ("Vishnu", 55, 8, "Short", "19/08/2019", "2014-09-27T23:55:45", "Transport"),
        ("Radha", 58, 9, "Long", "26/04/2016", "2015-05-13T15:35:25", "Safety"),
        ]
        
df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# Union operation
result_df = empty_df.union(df)
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **⚠️ Common Mistake**
# MAGIC - You must always pass a **schema** if the DataFrame is **empty**.

# COMMAND ----------

# This will raise an error because schema is not provided or mismatched
empty_df = spark.createDataFrame([])

# Error on this line:
result = empty_df.union(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Using loop with empty DataFrame for accumulation**

# COMMAND ----------

# Create an empty DataFrame with the same schema
df_loop = spark.createDataFrame([], df.schema)

for i in range(3):
    # Filter the original DataFrame based on Age
    filtered_df = df.filter(df.Age > i * 20)

    # Accumulate the results into df_loop
    df_loop = df_loop.union(filtered_df)

# Display the final accumulated DataFrame
display(df_loop)

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Chaining multiple unions with empty DataFrame as starting point**

# COMMAND ----------

# Assume df1, df2, df3 all have the same schema
df1 = spark.createDataFrame([(1, "Naresh", 28, "Medium"),
                             (2, "Mohan", 25, "Low"),
                             (3, "Hitesh", 29, "High"),
                             (4, "Vedita", 32, "Less"),
                             (5, "Sushmita", 35, "Higher")],
                             ["id", "Name", "Age", "Type"])

df2 = spark.createDataFrame([(1, "Neha", 21, "Medium"),
                             (2, "Mohin", 26, "Low"),
                             (3, "Hritik", 27, "High"),
                             (4, "Vasu", 35, "Less"),
                             (5, "Susi", 37, "Higher")],
                             ["id", "Name", "Age", "Type"])
df3 = spark.createDataFrame([(1, "Druv", 31, "Medium"),
                             (2, "Eashwar", 33, "Low"),
                             (3, "Guru", 29, "High"),
                             (4, "Vishwak", 39, "Less"),
                             (5, "Sophia", 41, "Higher")],
                             ["id", "Name", "Age", "Type"])

# Start with an empty DataFrame
final_df = spark.createDataFrame([], df1.schema)
display(final_df)

# COMMAND ----------

# Union multiple DataFrames
final_df = final_df.union(df1).union(df2).union(df3)
display(final_df)