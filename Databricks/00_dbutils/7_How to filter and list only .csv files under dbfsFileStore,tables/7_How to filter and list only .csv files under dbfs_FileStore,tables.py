# Databricks notebook source
# MAGIC %md
# MAGIC #### **Filtering Specific File Types**
# MAGIC
# MAGIC - If you're interested in files of a **specific type**, for example, **.csv** files, you can filter the results:

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,List down all files and folders
# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/")

# COMMAND ----------

# MAGIC %md
# MAGIC why anyone wants to use the **dbutils package**, instead of the **Magic Command %fs** in a databricks notebook?
# MAGIC - dbutils package provides greater flexibility as it can be combined with other native programming languages like **python, R, SCALA**.

# COMMAND ----------

# DBTITLE 1,output in tabular format
# output in tabular format
display(dbutils.fs.ls("dbfs:/FileStore/tables/"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 1: Using Databricks Utilities (dbutils)**
# MAGIC - **dbutils.fs.ls()** to list files and filter **.csv** files.

# COMMAND ----------

# List all files in the directory
files = dbutils.fs.ls("dbfs:/FileStore/tables/")

# COMMAND ----------

# DBTITLE 1,filter only .csv files
# Filter only CSV files
csv_files = [file for file in files if file.name.endswith('.csv')]

for csv_file in csv_files:
    print(csv_file)

# COMMAND ----------

# DBTITLE 1,filter only .csv files
# Filter only CSV files
csv_files_path = [file.path for file in files if file.name.endswith(".csv")]
print(csv_files_path)

# COMMAND ----------

# DBTITLE 1,display csv files
# Display CSV file paths
for csv_file in csv_files_path:
    print(csv_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Accessing Specific File Attributes**
# MAGIC
# MAGIC - Each object returned by dbutils.fs.ls has attributes like **path, name, size, and modificationTime**. You can access these individually:

# COMMAND ----------

for file in files:
    print(f"Path: {file.path}, Name: {file.name}, Size: {file.size}, Modified: {file.modificationTime}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 2: Using PySpark (spark.read.format())**

# COMMAND ----------

# Read all files from directory
df = spark.read.format("csv").option("header", True).load("dbfs:/FileStore/tables/*")
display(df)

# COMMAND ----------

from pyspark.sql.functions import input_file_name

# Add a column with file names
df = df.withColumn("file_path", input_file_name())

# Show unique file paths (CSV files)
df.select("file_path").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **databricks-datasets**

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# DBTITLE 1,databricks-datasets
# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %md
# MAGIC **a) airlines**

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/airlines/')

# COMMAND ----------

for files in dbutils.fs.ls('dbfs:/databricks-datasets/airlines/'):
    print (files)

# COMMAND ----------

for files in dbutils.fs.ls('dbfs:/databricks-datasets/airlines/'):
    print (files.name)

# COMMAND ----------

# MAGIC %md
# MAGIC **b) cctvVideos**

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/cctvVideos/')

# COMMAND ----------

for files in dbutils.fs.ls('dbfs:/databricks-datasets/cctvVideos/'):
    if files.name.endswith('/'):
        print (files.name)
