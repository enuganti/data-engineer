# Databricks notebook source
from pyspark.sql.functions import first

# COMMAND ----------

from pyspark.sql.types import MapType, StringType, IntegerType
from pyspark.sql import Row

# A list of rows, each with a map
data = [
    Row(name="Albert", properties={"age": 25, "height": 12, "Gender": "Male", "Country": "India", "eyeColor": "blue"}),
    Row(name="Bobby", properties={"age": 30, "height": 15, "Gender": "Male", "Country": "India", "eyeColor": None}),
    Row(name="Anand", properties={"age": 28, "height": 18, "Gender": "Male", "Country": "India", "eyeColor": "black"}),
    Row(name="Chandra", properties={"age": None, "height": 22, "Gender": "Male", "Country": "India", "eyeColor": "white"}),
    Row(name="Seetha", properties={"age": 45, "height": 32, "Gender": "Male", "Country": "India", "eyeColor": "yellow"}),
    Row(name="Varun", properties={"age": 32, "height": 10, "Gender": "Male", "Country": "India", "eyeColor": None}),
    Row(name="Bobby", properties={"age": None, "height": 19, "Gender": "Male", "Country": "India", "eyeColor": "blue"}),
    Row(name="Bobby", properties={"age": 30, "height": 8, "Gender": "Male", "Country": "India", "eyeColor": "brown"}),
    Row(name="Anand", properties={"age": None, "height": 16, "Gender": "Male", "Country": "India", "eyeColor": None}),
    Row(name="Chandra", properties={"age": 35, "height": 13, "Gender": "Male", "Country": "India", "eyeColor": "white"}),
    Row(name="Seetha", properties={"age": 45, "height": 27, "Gender": "Male", "Country": "India", "eyeColor": "yellow"}),
    Row(name="Anand", properties={"age": 32, "height": 29, "Gender": "Male", "Country": "India", "eyeColor": None})
]

# Define the schema
schema = ["name", "properties"]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Using `select()` and `col()` Functions**

# COMMAND ----------

from pyspark.sql.functions import col

# Select the name column, and the entries of the map as separate columns
df_col = df.select(
    "name",
    col("properties")["age"].alias("age"),
    col("properties")["eyeColor"].alias("eyeColor"),
    col("properties")["height"].alias("height"),
    col("properties")["Gender"].alias("Gender"),
    col("properties")["Country"].alias("Country")
)

display(df_col)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Using `explode()` Function**
# MAGIC
# MAGIC - To turn each **key-value pair** into a **separate row** and then **pivot** the data to create columns.

# COMMAND ----------

from pyspark.sql.functions import explode

# Explode the map into a new row for each key-value pair
df_exploded = df.select("name", explode("properties"))

display(df_exploded)

# COMMAND ----------

# Pivot the DataFrame to have distinct keys as separate columns
df_pivoted = df_exploded.groupBy("name").pivot("key").agg(first("value"))
display(df_pivoted)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Handling Null Values**
# MAGIC
# MAGIC - Null values in the map can cause issues when trying to access map keys directly. To deal with nulls, you can use the `coalesce()` function in combination with `lit()` to provide a default value where necessary

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit

df_with_defaults = df.select(
    "name",
    coalesce(col("properties")["age"], lit(0)).alias("age"),
    coalesce(col("properties")["eyeColor"], lit("unknown")).alias("eyeColor")
)

display(df_with_defaults)