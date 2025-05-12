# Databricks notebook source
# MAGIC %md
# MAGIC -  How to create an **empty PySpark DataFrame/RDD** manually **with or without schema** (column names) in different ways

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Create Empty DataFrame with / without Schema

# COMMAND ----------

# DBTITLE 1,Create Empty DataFrame without Schema (no columns)
# Create an empty StructType
schema = StructType()

# Create empty DatFrame with no schema (no columns)
df_empty = spark.createDataFrame([], schema=schema)
display(df_empty)
df_empty.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01:** Add fields using .add() method
# MAGIC      
# MAGIC      # Create an empty StructType
# MAGIC      schema = StructType()
# MAGIC
# MAGIC      # Add fields to the schema
# MAGIC      schemalist = schema.add("name", StringType(), True)
# MAGIC      schemalist = schema.add("age", IntegerType(), True)
# MAGIC
# MAGIC      print(schema)
# MAGIC
# MAGIC **Method 02:** Add multiple fields at once by chaining .add()
# MAGIC
# MAGIC      schema = StructType().add("id", IntegerType()).add("email", StringType())
# MAGIC      print(schema)
# MAGIC
# MAGIC **Method 03:** Create StructFields separately and build StructType
# MAGIC
# MAGIC      fields = [
# MAGIC          StructField("username", StringType(), True),
# MAGIC          StructField("score", IntegerType(), False)
# MAGIC      ]
# MAGIC
# MAGIC      schema = StructType(fields)
# MAGIC      print(schema)
# MAGIC

# COMMAND ----------

# Create an empty StructType
schema = StructType()

# Add fields to the schema
schemaList = schema.add("name", StringType(), True)
schemaList = schema.add("age", IntegerType(), True)

print(schema)

# COMMAND ----------

# MAGIC %md
# MAGIC **Output:**
# MAGIC
# MAGIC      StructType([
# MAGIC          StructField('name', StringType(), True),
# MAGIC          StructField('age', IntegerType(), True)
# MAGIC      ])
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create a DataFrame using this schema
df_empty_data = spark.createDataFrame([], schema=schemaList)
display(df_empty_data)
df_empty_data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Add fields one by one using .add()

# COMMAND ----------

# Create an empty StructType
schema = StructType()

# Add fields to the schema
schemaList = schema.add("name", StringType(), True)
schemaList = schema.add("age", IntegerType(), True)

# Use it in DataFrame creation
data = [("Harish", 30), ("Ramesh", 25), ("Swapna", 29), ("Swetha", 35), ("Anand", 26)]
df_add = spark.createDataFrame(data, schema=schemaList)
display(df_add)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Add multiple fields using a list of StructFields

# COMMAND ----------

fields = [
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
]

schemaList = StructType(fields)

# Example usage
data = [("Harish", 30), ("Ramesh", 25), ("Swapna", 29), ("Swetha", 35), ("Anand", 26)]

df_add_mltpl = spark.createDataFrame(data, schema=schemaList)
display(df_add_mltpl)
df_add_mltpl.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Add nested structures

# COMMAND ----------

address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("pincode", IntegerType(), True)
])

# Create an empty StructType
schema = StructType()

# Add fields to the schema
schemaList = schema.add("name", StringType(), True)
schemaList = schema.add("age", IntegerType(), True)
schemaList = schema.add("languagesAtSchool", ArrayType(StringType()))
schemaList = schema.add("languagesAtWork", MapType(StringType(), StringType(), True))
schemaList = schema.add("Properties", MapType(StringType(), IntegerType(), True))
# Add nested structure
schemaList_nested = schemaList.add("address", address_schema, True)

# Example usage
data = [("Harish", 30, ["Spark", "Java", "C++"], {"Domain": "Gas", "Branch": "IT", "Designation": "DE"}, {"Age": 25, "emp_id": 768954, "Exp": 5}, ("27th Main", "Bangalore", 5132109)),
        ("Ramesh", 25, ["Java", "Scala", "C++"], {"Domain": "DS", "Branch": "CSC", "Designation": "DE"}, {"Age": 30, "emp_id": 768956, "Exp": 2}, ("3rd cross", "Hyderabad", 5674321)),
        ("Swapna", 29, ["Devops", "VB"], {"Domain": "Trade", "Branch": "EEE", "Designation": "DE"}, {"Age": 28, "emp_id": 798954, "Exp": 8}, ("4th cross", "Chennai", 49087654)),
        ("Swetha", 35, ["CSharp", "VB", "Python"], {"Domain": "Sales", "Branch": "AI", "Designation": "DE"}, {"Age": 35, "emp_id": 788956, "Exp": 6}, ("4th Avenue", "Delhi", 4532167)),
        ("Anand", 26, ["PySpark", "SQL"], {"Domain": "TELE", "Branch": "ECE", "Designation": "DE"}, {"Age": 21, "emp_id": 769954, "Exp": 9}, ("5th Avenue", "Mumbai", 5760981))
        ]
df_nested = spark.createDataFrame(data, schema=schemaList_nested)
display(df_nested)