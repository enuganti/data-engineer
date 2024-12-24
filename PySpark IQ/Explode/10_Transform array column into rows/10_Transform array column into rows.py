# Databricks notebook source
# MAGIC %md
# MAGIC **Problem Statement**
# MAGIC
# MAGIC - You have a dataset containing **employee information**, where each employee may have **multiple technology experience**  stored in a **single column as an array**. Write a Pyspark code to **transform** this dataset so that each experience for each employee appears on a **separate row**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution**

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import col, explode

# COMMAND ----------

# Sample data
data = [(1, "Jayesh", "Tendulkar", 101, ['SQL','Data Science','PySpark']),
        (2, "Rohit", "Sharma", 102, ['Data Analytics','ML','AI']),
        (3, "Sai", "Ramesh", 101, ['SSMS','Azure','AWS','DEVOPS']),
        (4, "Sreedhar", "Arava", 102, ['Database','Oracle','ADF']),
        (5, "Somesh", "yadav", 101, ['SQL','Data Science','GitHub','PANDAS']),
        (6, "Radhika", "Gupta", 102, ['DEVOPS','AWS','SSMS','Python'])
       ]

columns = ["emp_id", "first_name", "last_name", "dept_id", "Technology"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
display(df)

# display data types
df.printSchema()

# COMMAND ----------

# Explode "Technology" column
exp_df = df.withColumn("Domain", explode(df.Technology))

# Display the result
display(exp_df)

# COMMAND ----------

# Explode "Technology" column
exploded_df = df.withColumn("Domain", explode(df.Technology)).drop("Technology")

# Display the result
display(exploded_df)

# display data types
df.printSchema()
