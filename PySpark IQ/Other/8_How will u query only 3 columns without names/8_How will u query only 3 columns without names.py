# Databricks notebook source
# MAGIC %md
# MAGIC **PROBLEM STATEMENT**
# MAGIC
# MAGIC - How will u query only **3 columns** of a table **without mentioning** the column name using pyspark?

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/Sales_Collect.csv", header=True, inferSchema=True)
display(df.limit(10))
df.printSchema()
print("Number of Rows:", df.count())

print("Column Names:\n\n", df.columns)
print("\n Number of columns:", len(df.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Select by Index**
# MAGIC - You can use a **Python list** to select columns by their **index**.

# COMMAND ----------

# Selects the first 3 columns
df.select(df.columns[:3]).show(3)

# Selects columns 2 to 4
df.select(df.columns[2:5]).show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Select All Columns from a List**
# MAGIC - If you have a **list of column names**, you can select all those columns using the ***operator**

# COMMAND ----------

# Your list of column names
columns = ["Id", "Vehicle_Id", "Description","Average"]
display(df.select(*columns).limit(10))
