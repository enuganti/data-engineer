# Databricks notebook source
# MAGIC %md
# MAGIC #### **StructType**
# MAGIC
# MAGIC - Each row in the STRUCT column must have the **same keys**.

# COMMAND ----------

from pyspark.sql.functions import lit, col, to_json
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, MapType

# COMMAND ----------

# Data
data = [
        (("jagadish", None, "Smith", 35, 5, "buy"),"chennai","M"),
        (("Anand", "Rose", "", 30, 8, "sell"), "bangalore", "M"),
        (("Julia", "", "Williams", 25, 3, "buy"), "vizak", "F"),
        (("Mukesh", "Bhat", "Royal", 45, 8, "buy"), "madurai", "M"),
        (("Swetha", "Kumari", "Anand", 55, 15, "sell"), "mysore", "F"),
        (("Madan", "Mohan", "Nair", 22, 11, "buy"), "hyderabad", "M"),
        (("George", "", "Williams", 38, 7, "sell"), "London", "M"),
        (("Roshan", "Bhat", "", 41, 3, "buy"), "mandya", "M"),
        (("Sourabh", "Sharma", "", 27, 2, "sell"), "Nasik", "M"),
        (("Mohan", "Rao", "K", 42, 7, "buy"), "nizamabad", "M")
        ]

# Schema
schema_arr = StructType([
    StructField('Name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True),
         StructField('age', IntegerType(), True),
         StructField('experience', IntegerType(), True),
         StructField('status', StringType(), True)
         ])),
     StructField('city', StringType(), True),
     StructField('gender', StringType(), True)
     ])

# Create DataFrame
df_arr = spark.createDataFrame(data=data, schema=schema_arr)
df_arr.printSchema()
display(df_arr)

# COMMAND ----------

# MAGIC %md
# MAGIC **Each row in the STRUCT column must have the same keys**

# COMMAND ----------

# Data
data = [
        (("jagadish", "Smith", 35, 5, "buy"),"chennai","M"),
        (("Anand", "Rose", "", 30, 8, "sell"), "bangalore", "M"),
        (("Julia", "", "Williams", 25, 3, "buy"), "vizak", "F"),
        (("Mukesh", "Bhat", "Royal", 45, 8, "buy"), "madurai", "M"),
        (("Swetha", "Kumari", "Anand", 55, 15, "sell"), "mysore", "F"),
        (("Madan", "Mohan", "Nair", 22, 11, "buy"), "hyderabad", "M"),
        (("George", "", "Williams", 38, 7, "sell"), "London", "M"),
        (("Roshan", "Bhat", "", 41, 3, "buy"), "mandya", "M"),
        (("Sourabh", "Sharma", "", 27, 2, "sell"), "Nasik", "M"),
        (("Mohan", "Rao", "K", 42, 7, "buy"), "nizamabad", "M")
        ]

# Schema
schema_arr = StructType([
    StructField('Name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True),
         StructField('age', IntegerType(), True),
         StructField('experience', IntegerType(), True),
         StructField('status', StringType(), True)
         ])),
     StructField('city', StringType(), True),
     StructField('gender', StringType(), True)
     ])

# Create DataFrame
df_arr = spark.createDataFrame(data=data, schema=schema_arr)
df_arr.printSchema()
display(df_arr)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **MapType**
# MAGIC
# MAGIC - **MapType** column is used to represent **key-value** pair data.
# MAGIC
# MAGIC       you want to store data for a person in key-value data structure
# MAGIC       {"name": "John", "age": 29, "emp_id": "123657"}

# COMMAND ----------

# Sample DataFrame with a StringType column containing JSON strings
data = [("Naresh", "Bangalore", {"Domain": "Gas", "Branch": "IT", "Designation": "DE", "Company": "TCS", "Mode": "Transport"}), 
        ("Harish", "Chennai", {"Domain": "DS", "Branch": "CSC", "Designation": "DE", "Company": "Sony"}),
        ("Prem", "Hyderabad", {"Domain": "Trade", "Branch": "EEE", "Designation": "DE"}), 
        ("Prabhav", "kochin", {"Domain": "Sales", "Branch": "AI"}),
        ("Hari", "Nasik", {"Domain": "TELE"}), 
        ("Druv", "Delhi", None),
        ]

# Define the schema for the MapType column
map_schema = StructType([
  StructField("Name", StringType(), True),
  StructField("City", StringType(), True),
  StructField("Properties", MapType(StringType(), StringType()), True)])

# Convert the StringType column to a MapType column
df_map = spark.createDataFrame(data, map_schema)

# Display the resulting DataFrame
display(df_map)