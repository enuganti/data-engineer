# Databricks notebook source
from pyspark.sql.functions import col, explode, explode_outer
import pyspark.sql.functions as f

# COMMAND ----------

data = [
    (1, "Suresh", [".net", "Python", "Spark", "Azure"]),
    (2, "Ramya", ["java", "PySpark", "AWS"]),
    (3, "Rakesh", ["ADF", "SQL", None, "GCC"]),
    (4, "Apurba", ["C", "SAP", "Mainframes"]),
    (5, "Pranitha", ["COBOL", "DEVOPS"]),
    (6, "Sowmya", ["ABAP"]),
    (7, "Anand", None),
    (8, "Sourabh", []),
]
schema = ["id", "Name", "skills"]

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()
print("Number of Rows:", df.count())

# COMMAND ----------

df.show(truncate=False)
display(df)

# COMMAND ----------

df = df.withColumn("New-Skills", explode_outer(col('skills')))
display(df)
df.printSchema()
