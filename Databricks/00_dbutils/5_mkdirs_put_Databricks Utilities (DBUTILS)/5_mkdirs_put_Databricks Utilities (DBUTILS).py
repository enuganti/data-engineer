# Databricks notebook source
# MAGIC %md
# MAGIC #### **dbutils.fs.mkdirs**: Create Directories and Files
# MAGIC
# MAGIC - Utility can be used to create **new directories** and **add new files/scripts** within the newly created directories.
# MAGIC
# MAGIC - The example below shows how **dbutils.fs.mkdirs()** can be used to create a **new directory** called **scripts** within “dbfs” file system.

# COMMAND ----------

dbutils.fs.help("mkdirs")

# COMMAND ----------

dbutils.fs.help("put")

# COMMAND ----------

# create directory "databricks"
# create script within directory "databricks"
dbutils.fs.mkdirs("dbfs:/databricks/scripts/")

# COMMAND ----------

dbutils.fs.rm("dbfs:/databricks", True)

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks/

# COMMAND ----------

# MAGIC %md
# MAGIC #### **dbutils.fs.put**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Format: json**

# COMMAND ----------

dbutils.fs.put("/databricks/scripts/multiLine_nested.json", """[
  {
    "source": "catalog",
    "description": "bravia",
    "input_timestamp": 1124256609,
    "last_update_timestamp": 1524256609,
    "Address":
             {
               "country": "IND",
               "user": "Hari",
               "Location":"Bangalore",
               "Zipcode":"560103"
             }
  },
  {
    "source": "SAP",
    "description": "sony",
    "input_timestamp": 1224256609,
    "last_update_timestamp": 1424256609,
    "Address":
             {
               "country": "US",
               "user": "Rajesh",
               "Location":"Chennai",
               "Zipcode":"860103"
             }
  },
  {
    "source": "ADLS",
    "description": "bse",
    "input_timestamp": 1324256609,
    "last_update_timestamp": 1524256609,
    "Address":
             {
               "country": "CANADA",
               "user": "Lokesh",
               "Location":"Hyderabad",
               "Zipcode":"755103"
             }
  },
  {
    "source": "Blob",
    "description": "exchange",
    "input_timestamp": 1424256609,
    "last_update_timestamp": 1724256609,
    "Address":
             {
               "country": "US",
               "user": "Sharath",
               "Location":"Kochin",
               "Zipcode":"120103"
             }
  },
  {
    "source": "SQL",
    "description": "Stock",
    "input_timestamp": 1524256609,
    "last_update_timestamp": 1664256609,
    "Address":
             {
               "country": "SWEDEN",
               "user": "Sheetal",
               "Location":"Delhi",
               "Zipcode":"875103"
             }
  },
  {
    "source": "datawarehouse",
    "description": "azure",
    "input_timestamp": 1624256609,
    "last_update_timestamp": 1874256609,
    "Address":
             {
               "country": "UK",
               "user": "Raj",
               "Location":"Mumbai",
               "Zipcode":"123403"
             }
  },
  {
    "source": "oracle",
    "description": "ADF",
    "input_timestamp": 1779256609,
    "last_update_timestamp": 188256609,
    "Address":
             {
               "country": "Norway",
               "user": "Synapse",
               "Location":"Nasik",
               "Zipcode":"456103"
             }
  }
]""", True)

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks/scripts/multiLine_nested.json

# COMMAND ----------

df = spark.read.json("dbfs:/databricks/scripts/multiLine_nested.json", multiLine=True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Format: txt**

# COMMAND ----------

data = "Name, Location, Domain, Country, Age\nSuresh, Bangalore, ADE, India, 25\nSampath, Bihar, Excel, India, 35\nKishore, Chennai, ADf, India, 28\nBharath, Hyderabad, Admin, India, 38\nBharani, Amaravathi, GITHUB, India, 45\nNiroop, Tituchi, Devops, India, 365\nSardar, Bangalore, JAVA, India, 32\nSwapnil, Bangalore, Automotive, India, 28\nRavi, Madurai, Python, India, 35"

dbutils.fs.put("/data/main_branch/Repo/project/sales.txt", data, True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/data", True)

# COMMAND ----------

# MAGIC %fs head /data/main_branch/Repo/project/sales.txt

# COMMAND ----------

dft = spark.read.csv("dbfs:/data/main_branch/Repo/project/sales.txt", header=True, inferSchema=True)
display(dft)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Format: CSV**

# COMMAND ----------

data_csv = "Index, Effective_Date, Start_Date	End_Date, Income, Delta_Value, Target_Id, Input_Timestamp_UTC, Update_Timestamp_UTC\n123, 6-Feb-23, 14-Jan-23, 6-Feb-23, 1500, 10, 1068, 1724256609000, 1724256609000\n124, 8-Jan-24, 7-Oct-23, 8-Jan-24, 1500, 10, 1068, 1724256609000, 1724256609000\n125, 6-Mar-23, 7-Feb-23, 6-Mar-23, 1500, 10, 1068, 1724256609000, 1724256609000\n126, 6-Jan-25, 9-Jan-24, 6-Jan-25, 1500, 10, 1068, 1724256609000, 1724256609000\n127, 31-Jan-24, 1-Jan-24, 31-Jan-24, 74, 12, 1065, 1724256609000, 1724256609000\n128, 31-Oct-24, 1-Oct-24, 31-Oct-24, 83, 12, 1065, 1724256609000, 1724256609000\n129, 30-Jun-24, 1-Jun-24, 30-Jun-24, 79, 11, 1065, 1724256609000, 1724256609000\n130, 9-Feb-23, 9-Feb-23, 9-Feb-23, 38, 17, 1071, 1724256609000, 1724256609000\n131, 23-Deb-23, 11-Deb-23, 25-Nov-23, 38, 17, 1071, 1724256609000, 1724256609000\n131, 23-Deb-23, 11-Deb-23, 25-Nov-23, 38, 17, 1071, 1724256609000, 1724256609000"

dbutils.fs.put("/data/main_branch/Repo/project/Marketing.csv", data_csv, True)

# COMMAND ----------

# MAGIC %fs head /data/main_branch/Repo/project/Marketing.csv

# COMMAND ----------

dfm = spark.read.csv("/data/main_branch/Repo/project/Marketing.csv", header=True, inferSchema=True)
display(dfm)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) Format: avro / parquet / orc**
# MAGIC
# MAGIC - **dbutils.fs.put** is primarily for writing **string or text** data to a file in DBFS (Databricks File System).
# MAGIC -  It **does not** directly support writing data in **binary formats** such as **Avro, parquet and orc**. 
