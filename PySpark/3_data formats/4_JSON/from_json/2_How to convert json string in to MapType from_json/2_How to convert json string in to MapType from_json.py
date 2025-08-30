# Databricks notebook source
# MAGIC %md
# MAGIC #### **How to convert json string in to MapType?**

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 01**

# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.types import MapType, StringType

# COMMAND ----------

data = [(123, '''{"Name":"Kiran"}''', "Sony")]
schema = ("id", "Format", "Name")

df1 = spark.createDataFrame(data, schema)
display(df1)

# COMMAND ----------

df_fj = df1.select(from_json(df1.Format, "MAP<STRING, STRING>").alias("parsed_json"))
display(df_fj)

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 02**

# COMMAND ----------

jsonString = """{"Zipcode":704, "ZipCodeType":"STANDARD", "City":"PARC PARQUE", "State":"PR"}"""
df2 = spark.createDataFrame([(1, jsonString)], ["id","value"])
display(df2)

# COMMAND ----------

# Convert JSON string column to Map type
schema01 = MapType(StringType(), StringType())

df2 = df2.withColumn("value", from_json(df2.value, schema01).alias('JSON'))
display(df2)
