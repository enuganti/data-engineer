# Databricks notebook source
df = spark.read.csv("/FileStore/tables/Streaming/Stream_readStream/csv/streaming_01.csv", header=True, inferSchema=True)

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
# MAGIC **Define schema for input csv file**
# MAGIC - schema must be specified when creating a streaming source dataframe, otherwise it will through error.

# COMMAND ----------

# DBTITLE 1,schema defined wrt csv
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the schema based on the CSV structure
schema_csv = StructType([
    StructField("Index", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Effective_Date", StringType(), True),
    StructField("Income_Level", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("TARGET", StringType(), True),
    StructField("Input_Timestamp_UTC", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) readStream**

# COMMAND ----------

# DBTITLE 1,Read csv files from input folder
stream_csv = spark.readStream\
                  .format("csv")\
                  .option("header", True)\
                  .schema(schema_csv)\
                  .csv("/FileStore/tables/Streaming/Stream_readStream/csv/")

print(stream_csv.isStreaming)
print(stream_csv.printSchema())
                        
display(stream_csv)

# stream_csv.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) writeStream**

# COMMAND ----------

# MAGIC %md
# MAGIC **format('console')**

# COMMAND ----------

stream_csv.writeStream\
                .format('console')\
                .outputMode('append')\
                .start()

display(stream_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC **format('parquet')**

# COMMAND ----------

stream_csv.writeStream\
                .format('parquet')\
                .outputMode('append')\
                .option("path", "/FileStore/tables/Streaming/Stream_writeStream/csv")\
                .option("checkpointLocation", "dbfs:/FileStore/tables/Streaming/Stream_checkpoint")\
                .start()

display(stream_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC **verify the written stream data**

# COMMAND ----------

display(spark.read.format("parquet").load("/FileStore/tables/Streaming/Stream_writeStream/csv/*.parquet"))
