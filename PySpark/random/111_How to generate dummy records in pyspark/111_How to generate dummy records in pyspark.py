# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, BooleanType, LongType
import string
import random
import datetime

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("Age", IntegerType(), True),
    StructField("department_id", StringType(), True),
    StructField("product_volume", DoubleType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("start_time", DateType(), True),
    StructField("source", StringType(), True),
    StructField("profile_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("index", BooleanType(), True),
    StructField("target", StringType(), True),
    StructField("impact", IntegerType(), True),
    StructField("impact1", IntegerType(), True),
    StructField("impact2", StringType(), True),
    StructField("start_date", LongType(), True),
    StructField("end_date", LongType(), True),
    StructField("update_date", LongType(), True)
])

# COMMAND ----------

# Generate random data
data = [
    {
        "Age": random.randint(1, 100),  # Random integer for Age
        "department_id": ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),  # Random alphanumeric string
        "product_volume": random.uniform(1, 100),  # Random float for product volume
        "Salary": random.randint(200, 500),  # Random integer for salary
        "start_time": datetime.date.today(),  # Today's date
        "source": f"RESTAPI_{i}",  # Custom string with index
        "profile_id": ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),  # Random alphanumeric string
        "category": random.choice(["standard", "upper", "medium", "premium", "lower"]),  # Random choice from a list
        "index": random.choice([True, False]),  # Boolean value
        "target": "Azure",  # Static value
        "impact": random.choice([0]), # integer column has all rows with "0"
        "impact1": random.choice([1]), # integer column has all rows with "1"
        "impact2": random.choice([""]), # column has empty values
        "start_date": int((datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365))).timestamp()),  # LongType
        "end_date": int((datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365))).timestamp()),  # LongType
        "update_date": int((datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365))).timestamp()),  # LongType
    }
    for i in range(20)
]

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema)
display(df)
