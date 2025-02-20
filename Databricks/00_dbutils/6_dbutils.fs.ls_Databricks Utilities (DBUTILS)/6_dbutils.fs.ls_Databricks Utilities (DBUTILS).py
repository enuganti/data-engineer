# Databricks notebook source
# MAGIC %md
# MAGIC #### **dbutils.fs.ls**
# MAGIC
# MAGIC - command is used to list the **contents of a directory** in DBFS (Databricks File System).

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help("ls")

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Listing Contents of the Root Directory**
# MAGIC
# MAGIC - To list the **contents** of the **root directory**, you can simply pass **"/"** as the path:
# MAGIC
# MAGIC       root_files = dbutils.fs.ls("/")
# MAGIC       for file in root_files:
# MAGIC           print(file)

# COMMAND ----------

# MAGIC %md
# MAGIC      # List Files and Directories
# MAGIC      %fs ls /
# MAGIC      dbutils.fs.ls("/")
# MAGIC      %sh ls /dbfs/
# MAGIC
# MAGIC      # Default Upload Directory
# MAGIC      %fs ls /FileStore/tables/
# MAGIC      dbutils.fs.ls("/FileStore/tables/") 
# MAGIC      %sh ls /dbfs/FileStore/tables/

# COMMAND ----------

# output in tabular format
%fs ls

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# output as List
dbutils.fs.ls('/')

# COMMAND ----------

# output in tabular format
display(dbutils.fs.ls('/'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Listing Contents of a Directory**
# MAGIC
# MAGIC - To list all files and directories within a specific directory, you can use:
# MAGIC
# MAGIC        files = dbutils.fs.ls("/mnt/your-directory/")
# MAGIC        for file in files:
# MAGIC            print(file)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/"))

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/databricks-datasets/")
for file in files:
    print(file)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-results/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/"))

# COMMAND ----------

# To check whether a folder has been deleted
display(dbutils.fs.ls("/FileStore"))

# COMMAND ----------

# To check whether a folder has been deleted
display(dbutils.fs.ls("/FileStore/tables/"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Check the folder and list the content**

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/"))
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/"))

# COMMAND ----------

# DBTITLE 1,Check the folder and list the content
# Check the folder and list the content
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/tbl_crs_join"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/InterviewQuestions/"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/InterviewQuestions/PySpark/"))

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/tbl_enriched_marketprice", recurse=true)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/Generate_Random_Data/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/"))
display(dbutils.fs.ls("dbfs:/user/hive/"))
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/"))
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/colors/"))
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/customer123/"))
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/customers/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/"))
display(dbutils.fs.ls("dbfs:/databricks-datasets/bikeSharing/"))
display(dbutils.fs.ls("dbfs:/databricks-datasets/bikeSharing/data-001/"))

display(dbutils.fs.ls("dbfs:/databricks-datasets/airlines/"))
display(dbutils.fs.ls("dbfs:/databricks-datasets/airlines/part-00000"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/"))

display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/Stream_checkpoint/"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/Stream_checkpoint/json/"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/Stream_checkpoint/json/Set02/"))
                      
display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/Stream_readStream/"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/Stream_readStream/json/"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/Stream_readStream/json/Set01/"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/Stream_readStream/json/Set02/"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/Stream_readStream/json/Set03/"))

display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/Stream_writeStream/"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/Stream_writeStream/json/"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/Stream_writeStream/json/Set01/"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/Streaming/Stream_writeStream/json/Set02/"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Listing Files Recursively**
# MAGIC
# MAGIC - There isn't a **direct way** to list files recursively with dbutils.fs.ls, but you can implement it using a **recursive function**.
# MAGIC
# MAGIC - This function demonstrate how to use **dbutils.fs.ls** to interact with DBFS, allowing you to **list and filter files and directories** efficiently within your Databricks notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC       def list_files_recursive(path):
# MAGIC           for file_info in dbutils.fs.ls(path):
# MAGIC               if file_info.isDir():  # Check if the file_info is a directory
# MAGIC                   list_files_recursive(file_info.path)  # Recursive call
# MAGIC               else:
# MAGIC                   print(file_info.path)
# MAGIC
# MAGIC       list_files_recursive("/mnt/your-directory/")

# COMMAND ----------

def list_files_recursive(path):
    for file_info in dbutils.fs.ls(path):
        if file_info.isDir():  # Check if the file_info is a directory
            list_files_recursive(file_info.path)  # Recursive call
        else:
            print(file_info.path)

list_files_recursive("dbfs:/FileStore/tables/")
