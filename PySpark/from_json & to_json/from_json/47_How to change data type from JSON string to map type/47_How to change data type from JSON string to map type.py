# Databricks notebook source
# MAGIC %md
# MAGIC **Key-Value Pairs**
# MAGIC - DataFrame with a column containing **JSON strings** representing **key-value pairs**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **How to convert string type columns into map type?**
# MAGIC
# MAGIC 1) JSON Structure
# MAGIC
# MAGIC 2) Nested JSON Structure
# MAGIC
# MAGIC 3) Handling Null Values
# MAGIC
# MAGIC 4) Realtime Scenario

# COMMAND ----------

# DBTITLE 1,import required libraries
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, MapType
from pyspark.sql.functions import from_json, col, coalesce, lit

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) JSON Structure**

# COMMAND ----------

# DBTITLE 1,create sample dataset
# Sample DataFrame with a StringType column containing JSON strings
data = [("Naresh", '{"Age": 25, "emp_id": 768954, "Exp": 5}'), 
        ("Harish", '{"Age": 30, "emp_id": 768956, "Exp": 2}'),
        ("Prem", '{"Age": 28, "emp_id": 798954, "Exp": 8}'), 
        ("Prabhav", '{"Age": 35, "emp_id": 788956, "Exp": 6}'),
        ("Hari", '{"Age": 21, "emp_id": 769954, "Exp": 9}'), 
        ("Druv", '{"Age": 36, "emp_id": 768946, "Exp": 4}'),
        ]

schema = ["Student_Name", "Properties"]

# Convert the StringType column to a MapType column
df_json = spark.createDataFrame(data, schema)

# Display the resulting DataFrame
display(df_json)

# COMMAND ----------

# Define the schema for the MapType column
map_schema = MapType(StringType(), IntegerType())

# Convert the StringType column to a MapType column
df_json = df_json.withColumn("json_map", from_json(col("Properties"), map_schema))

# Display the resulting DataFrame
display(df_json)

# COMMAND ----------

df_col = df_json.withColumn("age", df_json.json_map.Age)\
  .withColumn("Emp_ID", df_json.json_map.emp_id)\
  .withColumn("Exp", df_json.json_map.Exp)

display(df_col)

# COMMAND ----------

# Sample DataFrame with a StringType column containing JSON strings
data = [("Naresh", '{"Age": 25, "emp_id": 768954, "Exp": 5}'), 
        ("Harish", '{"Age": 30, "emp_id": "768956", "Exp": 2}'),
        ("Prem", '{"Age": 28, "emp_id": 798954, "Exp": 8}'), 
        ("Prabhav", '{"Age": 35, "emp_id": 788956, "Exp": "6"}'),
        ("Hari", '{"Age": 21, "emp_id": "769954", "Exp": 9}'), 
        ("Druv", '{"Age": 36, "emp_id": 768946, "Exp": 4}'),
        ]

schema = ["Student_Name", "Properties"]

# Convert the StringType column to a MapType column
df_json1 = spark.createDataFrame(data, schema)

# Define the schema for the MapType column
map_schema = MapType(StringType(), IntegerType())

# Convert the StringType column to a MapType column
df_json1 = df_json1.withColumn("json_map", from_json(col("Properties"), map_schema))

# Display the resulting DataFrame
display(df_json1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Nested JSON Structure**
# MAGIC - DataFrame with a column containing **JSON strings** representing **nested key-value pairs**.

# COMMAND ----------

# Sample DataFrame
data = [("1", '{"Name": "Hari", "map": {"Country": "India", "City": "Delhi", "Level": "Manager", "Designation": "DE"}}'), 
        ("2", '{"Name": "Narahari", "map": {"Country": "India", "City": "Delhi", "Level": "Manager", "Designation": "DS"}}'),
        ("3", '{"Name": "Venu", "map": {"Country": "India", "City": "Delhi", "Level": "Manager", "Designation": "Engineer"}}'),
        ("4", '{"Name": "Giri", "map": {"Country": "India", "City": "Delhi", "Level": "Manager", "Designation": "Admin"}}'), 
        ("5", '{"Name": "Sree", "map": {"Country": "India", "City": "Delhi", "Level": "Manager", "Designation":"Developer"}}'),
        ("6", '{"Name": "Anu", "map": {"Country": "India", "City": "Delhi", "Level": "Manager", "Designation": "Testing"}}'),
        ("7", '{"Name": "Devi", "map": {"Country": "India", "City": "Delhi", "Level": "Manager", "Designation": "Modeler"}}'), 
        ("8", '{"Name": "Kedar", "map": {"Country": "India", "City": "Delhi", "Level": "Manager", "Designation": "Sales"}}'),
        ("9", '{"Name": "Smith", "map": {"Country": "India", "City": "Delhi", "Level": "Manager", "Designation": "Executive"}}')
        ]
schema = ["id", "Profile"]

# Convert the StringType column to a MapType column
df_nest_json = spark.createDataFrame(data, schema)

# Display the DataFrame
display(df_nest_json)

# COMMAND ----------

# Define the schema for the nested JSON structure
nested_map_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("map", StructType([
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Level", StringType(), True),
        StructField("Designation", StringType(), True)
    ]), True)
])

# Convert the JSON string to nested MapType
df_nest_json = df_nest_json.withColumn("json_nest_map", from_json(col("Profile"), nested_map_schema))

# Display the DataFrame
display(df_nest_json)

# COMMAND ----------

df_nest_json_col = df_nest_json.withColumn("name", df_nest_json.json_nest_map.Name)\
  .withColumn("country", df_nest_json.json_nest_map.map.Country)\
  .withColumn("city", df_nest_json.json_nest_map.map.City)\
  .withColumn("level", df_nest_json.json_nest_map.map.Level)\
  .withColumn("designation", df_nest_json.json_nest_map.map.Designation)\
    .drop('id', 'Profile')

display(df_nest_json_col)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Handling Null Values**
# MAGIC - DataFrame with a column containing **JSON strings**, some of which might be **null**.

# COMMAND ----------

# DBTITLE 1,create sample dataset
# Sample DataFrame with a StringType column containing JSON strings
data = [("Naresh", '{"Country": "India", "City": "Delhi", "Level": "Manager"}'), 
        ("Harish", '{"Country": "USA", "City": "New York", "Level": "SrManager"}'),
        ("Prem", '{"Country": "UK", "City": "London", "Level": "GM"}'), 
        ("Prabhav", '{"Country": "Norway", "City": "Norths", "Level": "Executive"}'),
        ("Hari", '{"Country": "Sweden", "City": "Stockholm", "Level": "SrExecutive"}'), 
        ("Druv", None)
        ]

schema = ["Name", "Profile"]

# Convert the StringType column to a MapType column
df_null = spark.createDataFrame(data, schema)

# Display the DataFrame
display(df_null)

# COMMAND ----------

# DBTITLE 1,convert json string to map type
# Define the schema for the map
map_schema = MapType(StringType(), StringType())

# Convert the JSON string to MapType, handling null values
df_null = df_null.withColumn("json_null", from_json(col("Profile"), map_schema))

# Display the DataFrame
display(df_null)

# COMMAND ----------

# DBTITLE 1,replace empty JSON object
# Convert the JSON string to MapType, handling null values
# coalesce function to replace any null values in the json_str column with an empty JSON object ('{}')
df_null_lit = df_null.withColumn("json_null", from_json(coalesce(col("Profile"), lit('{}')), map_schema))

# Display the DataFrame
display(df_null_lit)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) Scenario**
# MAGIC - **Source files:** CSV & AVRO schema
# MAGIC - **Requirement:** Convert string data type to map type

# COMMAND ----------

# MAGIC %md
# MAGIC **AVRO Schema for Cust_Metadata**
# MAGIC
# MAGIC      {
# MAGIC        "name": "Cust_Metadata",
# MAGIC        "type": [
# MAGIC          "null",
# MAGIC          {
# MAGIC            "type": "map",
# MAGIC            "values": "string"
# MAGIC          }
# MAGIC        ],
# MAGIC        "doc": "additional key value pair, e.g Cust_Subgroup.",
# MAGIC        "default": null
# MAGIC      }

# COMMAND ----------

# MAGIC %md
# MAGIC **AVRO Schema for Price_Metadata**
# MAGIC
# MAGIC      {
# MAGIC        "name": "Price_Metadata",
# MAGIC        "type": [
# MAGIC          "null",
# MAGIC          {
# MAGIC            "type": "map",
# MAGIC            "values": "string"
# MAGIC          }
# MAGIC        ],
# MAGIC        "doc": "additional key value pair, e.g Company_Name, Category, Location & Cust_Type.",
# MAGIC        "default": null
# MAGIC      }

# COMMAND ----------

# MAGIC %md
# MAGIC **AVRO Schema for Additional_Metadata**
# MAGIC
# MAGIC      {
# MAGIC        "name": "Additional_Metadata",
# MAGIC        "type": [
# MAGIC          "null",
# MAGIC          {
# MAGIC            "type": "map",
# MAGIC            "values": "string"
# MAGIC          }
# MAGIC        ],
# MAGIC        "doc": "additional key value pair, e.g Cust_Category.",
# MAGIC        "default": null
# MAGIC      }

# COMMAND ----------

# DBTITLE 1,Read dataset
df = spark.read.csv("/FileStore/tables/StringToMaptype-1.csv", header=True, inferSchema=True)
display(df.limit(10))

# COMMAND ----------

# DBTITLE 1,Convert string to map type
# Convert string type (Cust_Metadata, Price_Metadata & Additional_Metadata) to a map type
df_str_map = df\
    .withColumn("Cust_Metadata", from_json(col("Cust_Metadata"), MapType(StringType(), StringType())))\
    .withColumn("Price_Metadata", from_json(col("Price_Metadata"), MapType(StringType(), StringType())))\
    .withColumn("Additional_Metadata", from_json(col("Additional_Metadata"), MapType(StringType(), StringType())))

display(df_str_map.limit(10))
