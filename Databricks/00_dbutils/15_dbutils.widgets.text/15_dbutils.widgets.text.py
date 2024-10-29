# Databricks notebook source
# MAGIC %md
# MAGIC - Databricks widgets are interactive input components that can be added to notebooks to **parameterize** them and make them more **reusable**. Widgets enable the passing of **arguments and variables** into notebooks in a simple way, **eliminating** the need to **hardcode values**.
# MAGIC
# MAGIC - Databricks widgets make notebooks **highly reusable** by letting users **change inputs dynamically** at runtime instead of having **hardcoded values**, which allows the same notebook to be used repeatedly with different configurations.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax**
# MAGIC
# MAGIC      # PySpark
# MAGIC      dbutils.widgets.text(name='your_name_text', defaultValue='Enter your name', label='Your name')
# MAGIC
# MAGIC      # SQL
# MAGIC      CREATE WIDGET TEXT table DEFAULT ""
# MAGIC      CREATE WIDGET TEXT filter_value DEFAULT "100"
# MAGIC
# MAGIC - You can **access** the **current value of the widget** or get a mapping of **all widgets**.
# MAGIC
# MAGIC       dbutils.widgets.get("state")
# MAGIC       dbutils.widgets.getAll()
# MAGIC
# MAGIC - you can **remove** a widget or all widgets in a notebook.
# MAGIC - If you **remove** a widget, you **cannot create** one in the **same cell**. You must create the widget in **another cell**.
# MAGIC
# MAGIC       dbutils.widgets.remove("state")
# MAGIC       dbutils.widgets.removeAll()
# MAGIC
# MAGIC       # SQL
# MAGIC       REMOVE WIDGET state

# COMMAND ----------

dbutils.widgets.help("text")

# COMMAND ----------

# DBTITLE 1,Method 01
dbutils.widgets.text(name='Set_Value', defaultValue='topic', label='Set Value')
print(dbutils.widgets.get("Set_Value"))

# COMMAND ----------

dbutils.widgets.text(name='Set_Value', defaultValue='topic', label='Set_Value')
print(dbutils.widgets.get("Set_Value"))

# COMMAND ----------

# DBTITLE 1,Method 02
dbutils.widgets.text(name='Rev_Value', defaultValue='', label='Rev Value')
print(dbutils.widgets.get("Rev_Value"))

# COMMAND ----------

# DBTITLE 1,Method 03
dbutils.widgets.text('Ver_Value', '', 'Ver Value')
print(dbutils.widgets.get("Ver_Value"))

# COMMAND ----------

dbutils.widgets.text('run_Value', '')
print(dbutils.widgets.get("run_Value"))

# COMMAND ----------

# DBTITLE 1,Remove Individual widget
dbutils.widgets.remove('Set_Value')

# COMMAND ----------

# DBTITLE 1,Remove All Widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Ex 01**

# COMMAND ----------

dbutils.widgets.text("kafka_topic", "", label="kafka topic")
kafka_topic =  dbutils.widgets.get("kafka_topic")  

dbutils.widgets.text("adls_prefix", "", label="adls prefix")
adls_prefix =  dbutils.widgets.get("adls_prefix")

# COMMAND ----------

dbutils.widgets.text("kafka_topic", "", label="kafka topic")
kafka_topic =  dbutils.widgets.get("kafka_topic")  

dbutils.widgets.text("adls_prefix", "", label="adls prefix")
adls_prefix =  dbutils.widgets.get("adls_prefix")  

if (kafka_topic == ""):
    dbutils.notebook.exit("Topic is mandatory")

if (adls_prefix == ""):
    dbutils.notebook.exit("adls_prefix is mandatory")

print("Topic - ",kafka_topic)
print("ADLS Prefix - ",adls_prefix)

# COMMAND ----------

dbutils.widgets.remove('kafka_topic')
dbutils.widgets.remove('adls_prefix')
# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Real Time Use CASE: 01**

# COMMAND ----------

df_text = spark.read.csv("dbfs:/FileStore/tables/to_timestamp-3.csv", header=True, inferSchema=True)
display(df_text.limit(10))

# COMMAND ----------

df_text.createOrReplaceTempView("marketing")

# COMMAND ----------

dbutils.widgets.text("view", "")
dbutils.widgets.text("filter_value", "")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ${view}
# MAGIC WHERE Status == ${filter_value}
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Real Time Use CASE: 02**

# COMMAND ----------

# DBTITLE 1,create widget with default value
# Create a text input widget with default value 'Rev01'
dbutils.widgets.text("Rev_Ver", "Rev01", "Rev Ver")

# Now, retrieve the value of the widget
Rev_Ver = '_' + dbutils.widgets.get('Rev_Ver')
display(Rev_Ver)

# COMMAND ----------

# DBTITLE 1,Create Text Input Widget with Empty Value
# Create a text input widget with empty value
dbutils.widgets.text("Source_Ver", "", "Source Ver")

try:
    Source_Ver = dbutils.widgets.get("Source_Ver")
    display(Source_Ver)
except Exception as e:
    Source_Ver = ''

print(Source_Ver)

# COMMAND ----------

# dbutils.fs.rm("/FileStore/tables/Streaming", True)

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_checkpoint/json/Set01")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_checkpoint/json/Set02")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_checkpoint/json/Set03")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_checkpoint/json/Set04")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_checkpoint/json/Set05")

dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_readStream/json/Set01")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_readStream/json/Set02")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_readStream/json/Set03")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_readStream/json/Set04")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_readStream/json/Set05")

dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_writeStream/json/Set01")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_writeStream/json/Set02")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_writeStream/json/Set03")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_writeStream/json/Set04")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_writeStream/json/Set05")

# COMMAND ----------

root_path

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import StringType,LongType

# Get file paths for Party
root_path = f"/FileStore/tables/Streaming/Stream_readStream/json/{Source_Ver}/"

# Read the input data from CSV file
sales_df = spark.read.option('header', True).option('InferSchema', True).csv(root_path)
display(sales_df)

# COMMAND ----------

# DBTITLE 1,Gas Physical Position Data
# get raw streaming checkpoint
# sales_raw_chkpoint = f"/FileStore/tables/Streaming/Stream_checkpoint/json/_chkpoint_tbl_raw_gas/{Source_Ver}/"
# print(sales_raw_chkpoint)

# get enriched streaming checkpoint
sales_enr_chkpoint = f"/FileStore/tables/Streaming/Stream_checkpoint/json/{Source_Ver}/_chkpoint_tbl_enriched_gas/"
print(sales_enr_chkpoint)

# get raw target table 
# raw_table = f"/FileStore/tables/Streaming/Stream_writeStream/json/tbl_src_raw_fact_tbl_01/{Source_Ver}"
# print(raw_table)

# get enriched target table 
# target_table = f"/FileStore/tables/Streaming/Stream_writeStream/json/tbl_src_enr_fact_tbl_01/{Source_Ver}"
# print(target_table)

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

stream_json = spark.readStream\
                   .option("multiline", "true")\
                   .format("json")\
                   .schema(schema_json)\
                   .json(f"/FileStore/tables/Streaming/Stream_readStream/json/{Source_Ver}")

print(stream_json.isStreaming)
print(stream_json.printSchema()) 

display(stream_json)

# stream_json.awaitTermination()

# COMMAND ----------

stream_json.writeStream\
           .format('parquet')\
           .outputMode('append')\
           .option("path", f"/FileStore/tables/Streaming/Stream_writeStream/json/{Source_Ver}")\
           .option("checkpointLocation", sales_enr_chkpoint)\
           .start()

display(stream_json)

# COMMAND ----------

# MAGIC %md
# MAGIC You can access the **current value** of the widget or get a mapping of **all widgets**:
# MAGIC
# MAGIC      dbutils.widgets.get("state")
# MAGIC      dbutils.widgets.getAll()

# COMMAND ----------

# DBTITLE 1,retrieve all widgets
# retrieve all widgets
dbutils.widgets.getAll()
