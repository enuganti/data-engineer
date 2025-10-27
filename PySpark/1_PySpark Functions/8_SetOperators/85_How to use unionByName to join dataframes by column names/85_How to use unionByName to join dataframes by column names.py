# Databricks notebook source
# MAGIC %md
# MAGIC #### How to use unionByName to join dataframes by column names?

# COMMAND ----------

# MAGIC %md
# MAGIC #### unionByName()
# MAGIC
# MAGIC - **union()** requires both DataFrames to have the **same schema** in the **same order**.
# MAGIC - **unionByName()** allows unioning by **matching column names** instead of relying on **order**.
# MAGIC - The **unionByName()** function in PySpark is used to **combine two or more DataFrames** based on their **column names**, rather than their **positional order**.
# MAGIC - This is a key **distinction** from the standard **union() or unionAll()** methods, which require the DataFrames to have **identical schemas** in terms of **both column names and order**.

# COMMAND ----------

from pyspark.sql.functions import lit, col, sum

# COMMAND ----------

# DBTITLE 1,Silver Table: 01
bronze_tbl_01 = spark.read.csv("/Volumes/@azureadb/pyspark/unionby/company_level.csv", header=True, inferSchema=True)
bronze_tbl = bronze_tbl_01.withColumn("session_id", col("session_id").cast("string"))

print("Column Names of Bronze Table 1: \n", bronze_tbl.columns)
print("\nNo of Columns in Bronze Table 1: ", len(bronze_tbl.columns))
print("\nTotal Rows in Bronze Table 01: ", bronze_tbl.count())

display(bronze_tbl)
# display(bronze_tbl.limit(10))

# COMMAND ----------

# DBTITLE 1,Silver Table: 02
silver_tbl = spark.read.csv("/Volumes/@azureadb/pyspark/unionby/device_level.csv", header=True, inferSchema=True)

print("Column Names of Silver Table 2: \n", silver_tbl.columns)
print("\nNo of Columns in Silver Table 2: ", len(silver_tbl.columns))
print("\nTotal Rows in Silver Table 02: ", silver_tbl.count())

display(silver_tbl)
# display(silver_tbl.limit(10))

# COMMAND ----------

diff_cols_bronze_silver = set(bronze_tbl.columns) - set(silver_tbl.columns)
print("Columns in Silver Table 1 but not in Silver Table 2: ", diff_cols_bronze_silver)

# COMMAND ----------

# DBTITLE 1,Bronze: Transformation
# step:1 adding GRANULARITY column to identify the level of dataframe
df_bronze_tbl = bronze_tbl.withColumn("GRANULARITY", lit("campaign"))
display(df_bronze_tbl)

# COMMAND ----------

# DBTITLE 1,Silver: Transformation
# step:2 adding GRANULARITY column to identify the level of dataframe
df_silver_tbl = silver_tbl \
    .withColumn("session_id", lit("NULL")) \
    .withColumn("session_name", lit("NULL")) \
    .withColumn("GRANULARITY", lit("device_category"))

display(df_silver_tbl)

# COMMAND ----------

# DBTITLE 1,Gold Table: unionByName
# step:3 union of bronze & silver dataframes
df_gold_tbl = df_bronze_tbl.unionByName(df_silver_tbl)
display(df_gold_tbl)

# COMMAND ----------

data = [
    ("Bronze Table", [row["GRANULARITY"] for row in df_bronze_tbl.select("GRANULARITY").distinct().collect()]),
    ("Silver Table", [row["GRANULARITY"] for row in df_silver_tbl.select("GRANULARITY").distinct().collect()]),
    ("Gold Table", [row["GRANULARITY"] for row in df_gold_tbl.select("GRANULARITY").distinct().collect()])
]

df_gran = spark.createDataFrame(data, ["Table", "Distinct GRANULARITY"])
display(df_gran)

# COMMAND ----------

# MAGIC %md
# MAGIC **.collect()**
# MAGIC - This collects **all the rows** from the DataFrame into a **Python list**.

# COMMAND ----------

data = [
    ("Bronze Table", [row["session_id"] for row in df_bronze_tbl.select("session_id").distinct().collect()]),
    ("Silver Table", [row["session_id"] for row in df_silver_tbl.select("session_id").distinct().collect()]),
    ("Gold Table", [row["session_id"] for row in df_gold_tbl.select("session_id").distinct().collect()])
]

df_sess_id = spark.createDataFrame(data, ["Table", "Distinct Sessions"])
display(df_sess_id)

# COMMAND ----------

data = [
    ("Bronze Table", [row["session_name"] for row in df_bronze_tbl.select("session_name").distinct().collect()]),
    ("Silver Table", [row["session_name"] for row in df_silver_tbl.select("session_name").distinct().collect()]),
    ("Gold Table", [row["session_name"] for row in df_gold_tbl.select("session_name").distinct().collect()])
]

df_sess_name = spark.createDataFrame(data, ["Table", "Distinct Session Name"])
display(df_sess_name)