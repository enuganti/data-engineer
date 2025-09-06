# Databricks notebook source
# MAGIC %md
# MAGIC #### **How to access MapType Elements?**

# COMMAND ----------

from pyspark.sql.functions import lit, col, create_map
from pyspark.sql.types import StructField, StructType, StringType, MapType

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

df_map = spark.read.csv("dbfs:/FileStore/tables/maptypeaccess.csv", header=True, inferSchema=True)
display(df_map.limit(10))

# COMMAND ----------

df_map = df_map\
    .withColumn("Product_Metadata", create_map(
        lit("Product_Subgroup"), col("Product_Subgroup")))\
    .withColumn("Price_Metadata", create_map(
        lit("Product_Price_Name"), col("Product_Price_Name"),
        lit("Product_Category"), col("Product_Category"),
        lit("Product_Label"), col("Product_Label"),
        lit("Delivery_Time"), col("Delivery_Time")))
    
display(df_map.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Method 01: Access elements of Map Type**

# COMMAND ----------

# MAGIC %md
# MAGIC      df = df.withColumn("Commodity_Subgroup", df_map.Product_Metadata["Product_Subgroup"])
# MAGIC                                          (or)
# MAGIC      df = df.withColumn("Commodity_Subgroup", df_map.Product_Metadata.Product_Subgroup)
# MAGIC

# COMMAND ----------

df_Subgp = df_map.select('Name', 'Product_Subgroup', 'Product_Metadata')\
                 .withColumn("Commodity_Subgroup", df_map.Product_Metadata.Product_Subgroup)
display(df_Subgp.limit(10))

# COMMAND ----------

df_data = df_map.select('Name', 'Price_Metadata')\
                 .withColumn("Prd_Price_Name", df_map.Price_Metadata.Product_Price_Name)\
                 .withColumn("Prd_Category", df_map.Price_Metadata.Product_Category)\
                 .withColumn("Prd_Label", df_map.Price_Metadata.Product_Label)\
                 .withColumn("Delvry_Time", df_map.Price_Metadata.Delivery_Time)\
                   
display(df_data.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Method 02: Access elements of Map Type using getItem**
# MAGIC
# MAGIC - Get the **keys and values** from MapType using **.getItem()**. 

# COMMAND ----------

# MAGIC %md
# MAGIC      df_map.select('Name', 'Price_Metadata')\
# MAGIC            .withColumn("Prd_Price_Name", df_map.Price_Metadata.getItem("Product_Price_Name"))\
# MAGIC            .withColumn("Prd_Category", df_map.Price_Metadata.getItem("Product_Category"))\
# MAGIC            .withColumn("Prd_Label", df_map.Price_Metadata.getItem("Product_Label"))\
# MAGIC            .withColumn("Delvry_Time", df_map.Price_Metadata.getItem("Delivery_Time"))\
# MAGIC            .drop("Price_Metadata")

# COMMAND ----------

df_getItem = df_map.select('Name', 'Price_Metadata')\
                 .withColumn("Prd_Price_Name", df_map.Price_Metadata.getItem("Product_Price_Name"))\
                 .withColumn("Prd_Category", df_map.Price_Metadata.getItem("Product_Category"))\
                 .withColumn("Prd_Label", df_map.Price_Metadata.getItem("Product_Label"))\
                 .withColumn("Delvry_Time", df_map.Price_Metadata.getItem("Delivery_Time"))
                   
display(df_getItem.limit(20))