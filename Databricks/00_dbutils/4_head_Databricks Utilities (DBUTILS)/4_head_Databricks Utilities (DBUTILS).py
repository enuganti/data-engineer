# Databricks notebook source
# MAGIC %md
# MAGIC **dbutils.fs.head()**
# MAGIC
# MAGIC - The dbutils.fs.head() method in Databricks is meant to read only the **initial bytes of a file**, so it is **not ideal** for **handling large files**.
# MAGIC
# MAGIC - This method is usually used to **quickly preview** the **content or structure** of a file.

# COMMAND ----------

# DBTITLE 1,Help on 'head'
dbutils.fs.help('head')

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) databricks-datasets**

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/structured-streaming

# COMMAND ----------

# DBTITLE 1,Streaming dataset
# MAGIC %fs ls /databricks-datasets/structured-streaming/events

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks-datasets/structured-streaming/events/file-0.json

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

# MAGIC %md
# MAGIC ### **2) Format: csv**

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 01: bikeSharing**
# MAGIC
# MAGIC      dbutils.fs.head("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv")
# MAGIC      dbutils.fs.head("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv", 1000)
# MAGIC      %fs head dbfs:/databricks-datasets/bikeSharing/data-001/day.csv

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/

# COMMAND ----------

# MAGIC %md
# MAGIC      %fs head dbfs:/databricks-datasets/bikeSharing/data-001/day.csv
# MAGIC                               (or)
# MAGIC      dbutils.fs.head("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv")

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks-datasets/bikeSharing/data-001/day.csv

# COMMAND ----------

dbutils.fs.head("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC - Utility can pull the **first few records** of a file using the **head** function, as shown below.
# MAGIC
# MAGIC - **dbutils.fs.head()** can be passed with **number of bytes** parameter to limit the data that gets printed out.
# MAGIC
# MAGIC - In the example below, the **first 1000 bytes** of a csv file are printed out.

# COMMAND ----------

dbutils.fs.head("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv", 1000)

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks-datasets/bikeSharing/data-001/day.csv

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 02: Read titanic dataset**
# MAGIC
# MAGIC      dbutils.fs.head("dbfs:/FileStore/tables/titanic.csv")
# MAGIC      dbutils.fs.head("dbfs:/FileStore/tables/titanic.csv", 100)
# MAGIC      %fs head 'dbfs:/FileStore/tables/titanic.csv'

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/titanic.csv")

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/titanic.csv", 200)

# COMMAND ----------

# MAGIC %fs head 'dbfs:/FileStore/tables/titanic.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC ### **3) Format: JSON**

# COMMAND ----------

# MAGIC %md
# MAGIC      dbutils.fs.head("/FileStore/tables/singleline-5.json")
# MAGIC      dbutils.fs.head("/FileStore/tables/singleline-5.json", 50)
# MAGIC      %fs head '/FileStore/tables/singleline-5.json'

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/singleline-5.json")

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/singleline-5.json", 43)

# COMMAND ----------

# MAGIC %fs head '/FileStore/tables/singleline-5.json'

# COMMAND ----------

# MAGIC %fs head 'dbfs:/FileStore/tables/multiline.json'

# COMMAND ----------

# MAGIC %md
# MAGIC ### **4) Format: txt**

# COMMAND ----------

# MAGIC %md
# MAGIC      dbutils.fs.head("dbfs:/FileStore/tables/cast_data.txt")
# MAGIC      dbutils.fs.head("dbfs:/FileStore/tables/cast_data.txt", 50)
# MAGIC      %fs head 'dbfs:/FileStore/tables/posexplode.txt'

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/posexplode.txt")

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/posexplode.txt", 50)

# COMMAND ----------

# MAGIC %fs head 'dbfs:/FileStore/tables/posexplode.txt'
