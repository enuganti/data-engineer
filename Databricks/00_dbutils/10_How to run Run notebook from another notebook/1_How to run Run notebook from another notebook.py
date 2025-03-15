# Databricks notebook source
# MAGIC %md
# MAGIC #### **How to run Run notebook from another notebook?**
# MAGIC
# MAGIC - The **%run** command allows you to **include another notebook within a notebook**.
# MAGIC - **%run** must be in a **cell by itself**, because it **runs the entire notebook** inline.
# MAGIC - You **cannot use %run** to run a **Python file** and **import the entities** defined in that file into a notebook.
# MAGIC - When you use **%run to run a notebook** that contains **widgets**, by default the specified notebook runs with the **widget’s default values**.

# COMMAND ----------

# MAGIC %md
# MAGIC      dbutils.notebook.run()
# MAGIC      %run

# COMMAND ----------

# MAGIC %fs ls FileStore/tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### **USE CASE: 01**

# COMMAND ----------

# MAGIC %run ./SparkSessionAppName

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/Sales_Collect.csv", header=True, inferSchema=True)
display(df.limit(5))

# COMMAND ----------

# MAGIC %run ./property

# COMMAND ----------

# MAGIC %run ./config_functions_01

# COMMAND ----------

# define spark configurations
shufflepartitions = getShufflePartitions()
spark.conf.set("spark.sql.shuffle.partitions", shufflepartitions)

# COMMAND ----------

shufflepartitions
