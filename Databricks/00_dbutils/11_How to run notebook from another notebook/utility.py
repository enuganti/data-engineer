# Databricks notebook source
# DBTITLE 1,Required Import Functions.
import re
import json
import time
import pandas as pd
from delta.tables import *
from pyspark.sql import Row
from datetime import datetime
from datetime import timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Defining Schema.
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, LongType

# Define custom schema for the nested structure
AddSchema = StructType([StructField('country', StringType(), False),
                        StructField('user', StringType(), False),
                        StructField('Location', StringType(), False),
                        StructField('Zipcode', StringType(), False),]
                      )
# Define the main schema including the nested structure
schema_json = StructType([StructField('source', StringType(), False),
                          StructField('description', StringType(), False),
                          StructField('input_timestamp', LongType(), False),
                          StructField('last_update_timestamp', LongType(), False),
                          StructField('Address', AddSchema)]
                        )

# COMMAND ----------

# DBTITLE 1,Function to read the file from Bronze Layer.
# Function to read the data from Bronze layer using custom schema
def read_from_bronze(bronze_layer_path):
  df = spark.read.option("multiLine", True).schema(schema_json).json(bronze_layer_path)
  return df
