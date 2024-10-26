# Databricks notebook source
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

df = spark.read.json('/FileStore/tables/Streaming/Stream_readStream/csv/multiline_nested.json', multiLine=True, schema=schema_json)
display(df)

# COMMAND ----------

df.write.parquet("/FileStore/tables/Streaming/Stream_readStream/parquet/multiline_nested.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC **Create folder in DBFS**

# COMMAND ----------

# DBTITLE 1,Create Directories
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_checkpoint/csv")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_checkpoint/json")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_checkpoint/parquet")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_checkpoint/orc")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_checkpoint/avro")

dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_readStream/csv/")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_readStream/json/")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_readStream/parquet/")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_readStream/orc/")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_readStream/avro/")

dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_writeStream/csv/")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_writeStream/json/")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_writeStream/parquet/")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_writeStream/orc/")
dbutils.fs.mkdirs("/FileStore/tables/Streaming/Stream_writeStream/avro/")

# COMMAND ----------

# MAGIC %md
# MAGIC **Delete folder in DBFS**

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/Streaming/Stream_checkpoint/csv", True)
dbutils.fs.rm("/FileStore/tables/Streaming/Stream_checkpoint/json", True)
dbutils.fs.rm("/FileStore/tables/Streaming/Stream_checkpoint/parquet", True)
dbutils.fs.rm("/FileStore/tables/Streaming/Stream_checkpoint/orc", True)
dbutils.fs.rm("/FileStore/tables/Streaming/Stream_checkpoint/avro", True)

dbutils.fs.rm("/FileStore/tables/Streaming/Stream_readStream/csv", True)
dbutils.fs.rm("/FileStore/tables/Streaming/Stream_readStream/json", True)
dbutils.fs.rm("/FileStore/tables/Streaming/Stream_readStream/parquet", True)
dbutils.fs.rm("/FileStore/tables/Streaming/Stream_readStream/orc", True)
dbutils.fs.rm("/FileStore/tables/Streaming/Stream_readStream/avro", True)

dbutils.fs.rm("/FileStore/tables/Streaming/Stream_writeStream/csv", True)
dbutils.fs.rm("/FileStore/tables/Streaming/Stream_writeStream/json", True)
dbutils.fs.rm("/FileStore/tables/Streaming/Stream_writeStream/parquet", True)
dbutils.fs.rm("/FileStore/tables/Streaming/Stream_writeStream/orc", True)
dbutils.fs.rm("/FileStore/tables/Streaming/Stream_writeStream/avro", True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Define schema for input JSON file**
# MAGIC - schema must be specified when creating a streaming source dataframe, otherwise it will through error.

# COMMAND ----------

# DBTITLE 1,schema defined wrt JSON
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

# MAGIC %md
# MAGIC #### **1) readStream**

# COMMAND ----------

# DBTITLE 1,Read JSON files from input folder
stream_json = spark.readStream\
                   .format("json")\
                   .schema(schema_json)\
                   .json("/FileStore/tables/Streaming/Stream_readStream/json/")

print(stream_json.isStreaming)
print(stream_json.printSchema()) 

display(stream_json)

# stream_json.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) writeStream**

# COMMAND ----------

# MAGIC %md
# MAGIC **format('console')**

# COMMAND ----------

stream_json.writeStream\
           .format('console')\
           .outputMode('append')\
           .start()

display(stream_json)

# COMMAND ----------

# MAGIC %md
# MAGIC **format('parquet')**

# COMMAND ----------

check_point = "/FileStore/tables/Streaming/Stream_checkpoint/json"

stream_json.writeStream\
           .format('parquet')\
           .outputMode('append')\
           .option("path", "/FileStore/tables/Streaming/Stream_writeStream/json/")\
           .option("checkpointLocation", check_point)\
           .start()

display(stream_json)

# COMMAND ----------

# MAGIC %md
# MAGIC **verify the written stream data**

# COMMAND ----------

display(spark.read.format("parquet").load("/FileStore/tables/Streaming/Stream_writeStream/json/*.parquet"))
