# Databricks notebook source
# MAGIC %md
# MAGIC **How to verify schema mismatches between two tables?**
# MAGIC - How to find which **columns are present** in **source_tbl but missing in target_tbl**.
# MAGIC - It’s useful when you’re **comparing two dataframes** (during **ETL validation or schema comparison**).

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario:**
# MAGIC - when validating **schema consistency** before loading data from one system to another:
# MAGIC
# MAGIC   - **Source table:** Data extracted from a production database.
# MAGIC   - **Target table:** Destination schema in a data warehouse.
# MAGIC
# MAGIC - If your **source table** has **additional columns** like **'created_at', 'updated_at', or 'is_active'** that aren’t in the **target schema**, this code helps **identify** them quickly before you attempt to **merge or insert** data—preventing errors and ensuring data compatibility.

# COMMAND ----------

# DBTITLE 1,dataset 01
source_tbl = spark.read.csv("/Volumes/@azureadb/pyspark/unionby/company_level.csv", header=True, inferSchema=True)
display(source_tbl.limit(5))
print("Column Names: ", source_tbl.columns)
print("No of Columns: ", len(source_tbl.columns))

# COMMAND ----------

# DBTITLE 1,dataset 02
target_tbl = spark.read.csv("/Volumes/@azureadb/pyspark/unionby/device_level.csv", header=True, inferSchema=True)
display(target_tbl.limit(5))
print("Column Names: ", target_tbl.columns)
print("No of Columns: ", len(target_tbl.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Source Has Extra Columns**

# COMMAND ----------

# MAGIC %md
# MAGIC      source_tbl.columns = ['id', 'name', 'age', 'city']
# MAGIC      target_tbl.columns = ['id', 'name']
# MAGIC      
# MAGIC      Output:
# MAGIC      Missing columns in Target: {'age', 'city'}

# COMMAND ----------

# MAGIC %md
# MAGIC | source_tbl_columns | target_tbl_columns |
# MAGIC |--------------------|--------------------|
# MAGIC |  start_date        |   start_date       |
# MAGIC |  product_url       |   product_url      |
# MAGIC |  category          |   category         | 
# MAGIC |  default_group     |   default_group    |
# MAGIC |  source_target     |   source_target    |
# MAGIC |  cloud_flatform    |   cloud_flatform   |
# MAGIC |  session_id        |                    |
# MAGIC |  session_name      |                    |
# MAGIC |  status_name       |   status_name      |
# MAGIC |  status_type       |   status_type      |
# MAGIC |  sessions          |   sessions         |
# MAGIC |  product_id        |   product_id       |
# MAGIC |  load datetime     |   load datetime    |

# COMMAND ----------

missing_cols_in_target = set(source_tbl.columns) - set(target_tbl.columns)
print(f"Missing columns in Target: {missing_cols_in_target}") 

# COMMAND ----------

# MAGIC %md
# MAGIC - **session_name, session_id** are present in **source_tbl** but **not in target_tbl**.

# COMMAND ----------

missing_cols_in_source = set(target_tbl.columns) - set(source_tbl.columns)
print(f"Missing columns in Source: {missing_cols_in_source}") 

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC | target_tbl_columns | source_tbl_columns |
# MAGIC |--------------------|--------------------|
# MAGIC |  start_date        |   start_date       |
# MAGIC |  product_url       |   product_url      |
# MAGIC |  category          |   category         | 
# MAGIC |  default_group     |   default_group    |
# MAGIC |  source_target     |   source_target    |
# MAGIC |  cloud_flatform    |   cloud_flatform   |
# MAGIC |                    |   session_id       |
# MAGIC |                    |   session_name     |
# MAGIC |  status_name       |   status_name      |
# MAGIC |  status_type       |   status_type      |
# MAGIC |  sessions          |   sessions         |
# MAGIC |  product_id        |   product_id       |
# MAGIC |  load datetime     |   load datetime    |

# COMMAND ----------

def findMissingColumns(source_tbl, target_tbl):
    missing_cols = set(source_tbl.columns) - set(target_tbl.columns)
    return missing_cols

# COMMAND ----------

findMissingColumns(source_tbl, target_tbl)

# COMMAND ----------

findMissingColumns(target_tbl, source_tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Target Has Extra Columns**

# COMMAND ----------

# MAGIC %md
# MAGIC      source_tbl.columns = ["x", "y"]
# MAGIC      target_tbl.columns = ["x", "y", "z"]
# MAGIC
# MAGIC      missing_cols_in_target = set(source_tbl.columns) - set(target_tbl.columns)
# MAGIC      print(f"Missing columns in Target: {missing_cols_in_target}")
# MAGIC
# MAGIC      Missing columns in Target: set()
# MAGIC
# MAGIC - This code finds which **columns are present** in **source_tbl but missing in target_tbl**.
# MAGIC
# MAGIC - Even though **target** has an **extra column 'z'**, this code only checks for **columns missing in the target**, so it **ignores extra ones**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **3) No Missing Columns**

# COMMAND ----------

# MAGIC %md
# MAGIC      source_tbl.columns = ["a", "b", "c"]
# MAGIC      target_tbl.columns = ["a", "b", "c"]
# MAGIC
# MAGIC      missing_cols_in_target = set(source_tbl.columns) - set(target_tbl.columns)
# MAGIC      print(f"Missing columns in Target: {missing_cols_in_target}")
# MAGIC
# MAGIC      Missing columns in Target: set()
# MAGIC
# MAGIC **Both have the same columns, so the result is an empty set.**
# MAGIC