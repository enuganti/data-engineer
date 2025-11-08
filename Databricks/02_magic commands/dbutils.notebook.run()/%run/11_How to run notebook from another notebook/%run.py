# Databricks notebook source
dbutils.fs.mkdirs("dbfs:/FileStore/tables/source_to_bronze")

# COMMAND ----------

# DBTITLE 1,Import Utils function notebook.
# MAGIC %run ./utility

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/source_to_bronze/

# COMMAND ----------

bronze_layer_path = '/FileStore/tables/source_to_bronze/multiLine_nested_02.json'

# COMMAND ----------

# DBTITLE 1,Check if the file exists in the bronze layer
try:
  file_exists = dbutils.fs.ls(bronze_layer_path)
  if file_exists:
    file_found = True
except:
  file_found = False

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Read JSON File**
# MAGIC #### **Method 01**

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, LongType

# Define custom schema for the nested structure
AddSchema = StructType([StructField('country',StringType(),False),
                        StructField('user',StringType(),False),
                        StructField('Location',StringType(),False),
                        StructField('Zipcode',StringType(),False),]
                      )
# Define the main schema including the nested structure
schema_json = StructType([StructField('source',StringType(),False),
                          StructField('description',StringType(),False),
                          StructField('input_timestamp',LongType(),False),
                          StructField('last_update_timestamp',LongType(),False),
                          StructField('Address',AddSchema)]
                        )

# COMMAND ----------

# DBTITLE 1,Read the Json data with Custom Schema
if file_found:
    df_man = spark.read.json("/FileStore/tables/source_to_bronze/multiLine_nested_02.json", multiLine=True, schema=schema_json)
    display(df_man)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Method 02**

# COMMAND ----------

# DBTITLE 1,Read the Json data with Custom Schema
if file_found:
  df_run = read_from_bronze(bronze_layer_path)
  display(df_run)
