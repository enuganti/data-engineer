# Databricks notebook source
df = spark.read.orc("/FileStore/tables/Streaming/Stream_readStream/orc/multiline_nested_orc.orc")
display(df)

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

# DBTITLE 1,Remove Directories
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

schema_orc = StructType([StructField('source',StringType(),False),
                         StructField('description',StringType(),False),
                         StructField('input_timestamp',LongType(),False),
                         StructField('last_update_timestamp',LongType(),False),
                         StructField('country',StringType(),False),
                         StructField('user',StringType(),False),
                         StructField('Location',StringType(),False),
                         StructField('Zipcode',StringType(),False)]
                        )

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) readStream**

# COMMAND ----------

# DBTITLE 1,Read JSON files from input folder
stream_orc = spark.readStream\
                      .format("orc")\
                      .schema(schema_orc)\
                      .orc("/FileStore/tables/Streaming/Stream_readStream/orc/")

print(stream_orc.isStreaming)
print(stream_orc.printSchema())

display(stream_orc)

# stream_orc.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) writeStream**

# COMMAND ----------

# MAGIC %md
# MAGIC **format('parquet')**

# COMMAND ----------

check_point = "/FileStore/tables/Streaming/Stream_checkpoint/orc"

stream_orc.writeStream\
          .format('parquet')\
          .outputMode('append')\
          .option("path", "/FileStore/tables/Streaming/Stream_writeStream/orc/")\
          .option("checkpointLocation", check_point)\
          .start()

print(stream_orc.isStreaming)
print(stream_orc.printSchema())

display(stream_orc)

# stream_orc.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC **verify the written stream data**

# COMMAND ----------

display(spark.read.format("parquet").load("/FileStore/tables/Streaming/Stream_writeStream/orc/*.parquet"))
