# Databricks notebook source
# MAGIC %md
# MAGIC - To convert a **StructType (struct)** DataFrame column to a **MapType (map)** column in PySpark, you can use the **create_map** function.

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# COMMAND ----------

# DBTITLE 1,sample dataset
data = [("36636","Finance",(3000,"USA")), 
        ("40288","Finance",(5000,"IND")), 
        ("42114","Sales",(3900,"USA")), 
        ("39192","Marketing",(2500,"CAN")), 
        ("34534","Sales",(6500,"USA"))]

schema = StructType([
     StructField('id', StringType(), True),
     StructField('dept', StringType(), True),
     StructField('properties', StructType([
         StructField('salary', IntegerType(), True),
         StructField('location', StringType(), True)
         ]))
     ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Convert StructType to MapType (map) Column**

# COMMAND ----------

# Convert struct type to Map
from pyspark.sql.functions import col, lit, create_map

df_map = df.withColumn("propertiesMap", create_map(
  lit("salary"), col("properties.salary"),
  lit("location"), col("properties.location")
  )).drop("properties")

df.printSchema()
display(df)