# Databricks notebook source
# MAGIC %md
# MAGIC #### **How to delete Files from Databricks File System (DBFS)?**
# MAGIC
# MAGIC - **Removes** a **file or directory** and optionally all of its contents.
# MAGIC - If a **file** is specified, the **recurse** parameter is **ignored**.
# MAGIC - If a **directory** is specified, an **error** occurs if **recurse is disabled** and the directory is not empty.

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.fs.rm()**
# MAGIC
# MAGIC - How to remove Files?
# MAGIC - How to remove Folders?
# MAGIC - How to remove checkpoints?
# MAGIC - How to create folder and Remove folders?
# MAGIC
# MAGIC **dbutils.fs.mkdirs()**
# MAGIC
# MAGIC **dbutils.fs.put()**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax**
# MAGIC
# MAGIC      dbutils.fs.rm("/FileStore/tables/Show_truncate-1.csv")
# MAGIC                          (or)
# MAGIC      dbutils.fs.rm("/FileStore/tables/circuits.csv", recurse=False)
# MAGIC                          (or)
# MAGIC      %fs rm -r /FileStore/tables/Party_Relationship.csv
# MAGIC                          (or)
# MAGIC      %fs rm -r /FileStore/tables/Parquet_Userdata3
# MAGIC
# MAGIC        where,
# MAGIC               %fs magic command to use dbutils
# MAGIC               rm remove command
# MAGIC               -r recursive flag to delete a directory and all its contents
# MAGIC               /mnt/driver-daemon/jars/ path to directory

# COMMAND ----------

dbutils.fs.help("rm")

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Three ways to check for existance / deletion of file**
# MAGIC
# MAGIC - %fs ls /FileStore/tables/
# MAGIC
# MAGIC - read csv file --> df = spark.read.csv("dbfs:/FileStore/tables/MarketPrice-1.csv", header=True, inferSchema=True)
# MAGIC
# MAGIC - Catalog --> DBFS --> /FileStore/tables/ --> MarketPrice-1.csv

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

# To check whether a folder has been deleted
display(dbutils.fs.ls("/FileStore/tables/"))

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/temp_data/

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) How to remove Files?**

# COMMAND ----------

# MAGIC %md
# MAGIC      dbutils.fs.rm("/FileStore/tables/Show_truncate-1.csv")
# MAGIC                          (or)
# MAGIC      dbutils.fs.rm("/FileStore/tables/circuits.csv", recurse=False)
# MAGIC                          (or)
# MAGIC      %fs rm -r /FileStore/tables/Party_Relationship.csv

# COMMAND ----------

# MAGIC %md
# MAGIC **How to remove single file?**

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/MarketPrice-1.csv")

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/MarketPrice-1.csv", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **How to remove multiple files?**

# COMMAND ----------

# DBTITLE 1,Remove multiple files
dbutils.fs.rm("dbfs:/FileStore/tables/RunningData_Rev02.csv")
dbutils.fs.rm("dbfs:/FileStore/tables/Sales_Collect_Rev02.csv")
dbutils.fs.rm("dbfs:/FileStore/tables/StructType-1.csv")
dbutils.fs.rm("dbfs:/FileStore/tables/StructType-2.csv")
dbutils.fs.rm("dbfs:/FileStore/tables/StructType-3.csv")
dbutils.fs.rm("dbfs:/FileStore/tables/person-1.json")

# COMMAND ----------

# MAGIC %md
# MAGIC **How to check and delete files?**

# COMMAND ----------

# Check the folder and list the content
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/tbl_enriched_marketprice"))

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/tbl_enriched_marketprice", recurse=true)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) How to remove folder?**

# COMMAND ----------

# MAGIC %md
# MAGIC      dbutils.fs.rm("/FileStore/tables/Parquet_Userdata1", recurse=True)
# MAGIC                                  (or)
# MAGIC      dbutils.fs.rm("/FileStore/tables/Parquet_Userdata1", True)
# MAGIC                                  (or)
# MAGIC      %fs rm -r /FileStore/tables/Parquet_Userdata3
# MAGIC      where,
# MAGIC            %fs magic command to use dbutils
# MAGIC            rm remove command
# MAGIC            -r recursive flag to delete a directory and all its contents
# MAGIC            /mnt/driver-daemon/jars/ path to directory                            

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 01**

# COMMAND ----------

# DBTITLE 1,Remove Root folder
# remove root folder
dbutils.fs.rm("dbfs:/FileStore/tables/temp", recurse=True)

# COMMAND ----------

# DBTITLE 1,Remove sub-folder
# remove sub folder
dbutils.fs.rm("dbfs:/FileStore/tables/temp_data/data_16062024/", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 02**

# COMMAND ----------

data = "Name, Location, Domain, Country, Age\nSuresh, Bangalore, ADE, India, 25\nSampath, Bihar, Excel, India, 35\nKishore, Chennai, ADf, India, 28\nBharath, Hyderabad, Admin, India, 38\nBharani, Amaravathi, GITHUB, India, 45"

dbutils.fs.put("/Volumes/main/default/my-volume/hello.txt", data, True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/Volumes", True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) How to remove checkpoint?**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the schema based on the CSV structure
schema_csv = StructType([
    StructField("Id", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])

# COMMAND ----------

df_stream_Single = spark.readStream\
                        .format("csv")\
                        .option("header", True)\
                        .schema(schema_csv)\
                        .csv("/FileStore/tables/Streaming/Stream_readStream/")
                        
display(df_stream_Single)

# COMMAND ----------

checkpoint = "dbfs:/FileStore/tables/Streaming/Stream_checkpoint"

df_stream_Single.writeStream\
                .format('parquet')\
                .outputMode('append')\
                .option("path", "/FileStore/tables/Streaming/Stream_writeStream/")\
                .option("checkpointLocation", checkpoint)\
                .start()

display(df_stream_Single)

# COMMAND ----------

dbutils.fs.rm(checkpoint, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) Create folder and Remove folders**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **mkdirs command (dbutils.fs.mkdirs)**
# MAGIC
# MAGIC - Creates the given **directory if it does not exist**.

# COMMAND ----------

dbutils.fs.help("mkdirs")

# COMMAND ----------

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
