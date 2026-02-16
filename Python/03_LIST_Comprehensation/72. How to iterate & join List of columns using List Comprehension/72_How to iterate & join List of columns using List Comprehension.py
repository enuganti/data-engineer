# Databricks notebook source
# MAGIC %md
# MAGIC ##### How to iterate & join List of columns using List Comprehension?

# COMMAND ----------

target_cols = ['S.No','Target_ID','Target_version_id','sales_id']

target_cols = ', '.join([f't.{col} = s.{col}' for col in target_cols])
target_cols

# COMMAND ----------

target_cols = ['S.No','Target_ID','Target_version_id','sales_id','market_data_id','vehicle_delivery_start_date','vehicle_delivery_end_date','vehicle_delivery_payment_date','vehicle_spread','cluster_monitor','vehicle_price_determination_date','price_status']

# COMMAND ----------

# how to join source & target columns using join
# target_cols Specific columns
target_cols_updt_cols = ', '.join([f't.{col} = s.{col}' for col in target_cols])
target_cols_updt_cols

# COMMAND ----------

# List of column names from target_cols
# It creates a new list by looping through target_cols.
[col for col in target_cols]

# COMMAND ----------

# Create a list of source column references for SQL join/select
['s.{col}' for col in target_cols]

# COMMAND ----------

# MAGIC %md
# MAGIC - creates a **new list** of **strings**.
# MAGIC - For **every column name** inside **target_cols**, it builds a **new string starting with "s."**.
# MAGIC - But this code has a **small issue**:
# MAGIC   - `'s.{col}'` will not substitute the value of **col**.
# MAGIC   - It will literally produce: **"s.{col}", "s.{col}", …**

# COMMAND ----------

# Create a list of source column references for SQL join/select
[f"s.{col}" for col in target_cols]

# COMMAND ----------

# MAGIC %md
# MAGIC - when you want to **prefix columns** with an **alias**:
# MAGIC
# MAGIC       df.select([f"s.{col}" for col in target_cols])
# MAGIC
# MAGIC - This selects:
# MAGIC   - s.S.No
# MAGIC   - s.Target_ID
# MAGIC   - s.Target_version_id

# COMMAND ----------

# Create a list of target column references for SQL join/select
[f"t.{col}" for col in target_cols]

# COMMAND ----------

# Create a list of SQL assignment expressions for each column in target_cols
[f"t.{col} = s.{col}" for col in target_cols]

# COMMAND ----------

# MAGIC %md
# MAGIC - This is a **list comprehension** that creates a **list of SQL expressions**.
# MAGIC - For **every column name** in **target_cols**, it makes a **string** of this form:
# MAGIC
# MAGIC       t.columnName = s.columnName
# MAGIC
# MAGIC - Usually used in PySpark **merge or join** conditions.
# MAGIC
# MAGIC       merge_condition = " AND ".join([f"t.{col} = s.{col}" for col in target_cols])
# MAGIC
# MAGIC   - This creates a merge condition that **compares columns** of target alias **t** with columns of source alias **s**.