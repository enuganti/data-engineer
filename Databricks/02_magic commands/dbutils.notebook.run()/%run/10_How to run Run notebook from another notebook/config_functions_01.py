# Databricks notebook source
def getShufflePartitions():
  shf_parts = DevConfig['General']['shuffle_partitons']
  return shf_parts

# COMMAND ----------

def getMinPartitions():
  min_parts = DevConfig['General']['min_partitions']
  return min_parts
