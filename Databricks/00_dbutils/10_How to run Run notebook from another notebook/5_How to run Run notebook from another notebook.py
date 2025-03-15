# Databricks notebook source
# MAGIC %md
# MAGIC #### **USE CASE: 05**

# COMMAND ----------

# MAGIC %run ./Support_Notebooks/config_functions_02

# COMMAND ----------

# define spark configurations
shufflepartitions = getShufflePartitions()
spark.conf.set("spark.sql.shuffle.partitions", shufflepartitions)

# COMMAND ----------

shufflepartitions
