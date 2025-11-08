# Databricks notebook source
# MAGIC %md
# MAGIC #### **USE CASE: 02**

# COMMAND ----------

# MAGIC %run ./config_functions_02

# COMMAND ----------

# define spark configurations
shufflepartitions = getShufflePartitions()
spark.conf.set("spark.sql.shuffle.partitions", shufflepartitions)

# COMMAND ----------

shufflepartitions
