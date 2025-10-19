# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Creating a DataFrame with None values**

# COMMAND ----------

data = [("Arnab", 31, 10, 30000),
        ("Henry", None, 8, 25000),
        ("Jayesh", 29, 4, None),
        ("Jagadish", 24, 3, 20000),
        ("Kamalesh", 21, None, 15000),
        ("Lepakshi", 23, 2, 18000),
        ("Anand", None, None, 40000),
        ("NULL", 34, 10, 38000),
        ("NULL", 36, None, None)]

columns = ["Employee Name", "Age of Employee", "Experience (in years)", "Salary (per month - $)"]

df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Using .withColumn() to add a NULL column**

# COMMAND ----------

from pyspark.sql.functions import lit

df_with_null = df.withColumn("Value", lit(None)) \
                 .withColumn("Sales", lit(None).cast(IntegerType()))
display(df_with_null)

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Creating a DataFrame from Row objects with NULLs**

# COMMAND ----------

from pyspark.sql import Row

data = [
    Row(id=1, name="Alekya"),
    Row(id=None, name="Bibin"),
    Row(id=3, name=None),
    Row(id=None, name=None)
]

df_from_rows = spark.createDataFrame(data)
display(df_from_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Creating empty DataFrame with NULL placeholders**

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])

# Just placeholders with all nulls
empty_data = [(None, None), (None, None), (None, None), (None, None), (None, None)]

df_null_placeholder = spark.createDataFrame(empty_data, schema)
display(df_null_placeholder)