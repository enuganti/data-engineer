# Databricks notebook source
# MAGIC %md
# MAGIC ##### How to convert the values of a column into a Python list?

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Using .collect()
# MAGIC - This is the simplest method, but it **pulls all data to the driver**, so it’s only good for **small datasets**.

# COMMAND ----------

df = spark.read.csv("/Volumes/@azureadb/pyspark/unionby/company_level.csv", header=True, inferSchema=True)
display(df.limit(15))

# COMMAND ----------

status_type = df.select("status_type").collect()
status_type

# COMMAND ----------

# Convert a column into a Python list
values_list = [row["status_type"] for row in df.select("status_type").collect()]
print(values_list)

# COMMAND ----------

# MAGIC %md
# MAGIC - **df.select("column_name")** → selects only the **status_type** column from the DataFrame.
# MAGIC
# MAGIC - **.collect()** → brings **all rows** of the **selected column** to the **driver** as a **list of Row objects**.
# MAGIC
# MAGIC - The list comprehension **[row["status_type"] for row in ...]** extracts the value of **status_type** from **each Row** and creates a **Python list**.
# MAGIC
# MAGIC **When to use which method:**
# MAGIC - **.collect()** should only be used for **small datasets**, as it loads **all data** into the **driver’s memory**.
# MAGIC - For **small datasets** where you need the **entire column** as a **Python list on the driver**, collect() with **list comprehension or toPandas()** are convenient.
# MAGIC - For **large datasets**, avoid **.collect() or .toPandas()**.
# MAGIC - For **larger datasets**, especially when you need to perform further operations within Spark, using **collect_list() or collect_set()** for **aggregation** is more efficient as the operation remains distributed.
# MAGIC - Instead, try to work in **Spark** directly without converting to a Python list, or use **take(n)** if you only need a sample.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Getting only distinct values
# MAGIC - **.distinct()** removes **duplicates** before collecting.

# COMMAND ----------

distinct_list = [row["status_type"] for row in df.select("status_type").distinct().collect()]
print(distinct_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Using toPandas() (not recommended for large datasets)

# COMMAND ----------

values_list = df.select("status_type").toPandas()["status_type"].tolist()
print(values_list)

# COMMAND ----------

# MAGIC %md
# MAGIC - Converts **DataFrame to Pandas**, then uses Pandas **.tolist()**.
# MAGIC - Works but requires **memory to fit** the whole DataFrame on the **driver**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Using collect_list() or collect_set() (for aggregation)
# MAGIC - collect_list() (with duplicates)
# MAGIC - collect_set() (unique values)

# COMMAND ----------

from pyspark.sql import functions as F

data = [("A", 1), ("A", 1), ("A", 2), ("B", 3), ("B", 1), ("C", 1), ("C", 2), ("C", 2), ("C", 3), ("C", 3), ("D", 2), ("D", 3), ("D", 3), ("E", 1), ("E", 2), ("E", 2)]

df = spark.createDataFrame(data, ["Category", "Value"])
display(df)

# COMMAND ----------

# Collect values into a list for each category
df_collected = df.groupBy("Category").agg(F.collect_list("Value").alias("ValueList"))
df_collected.display()

# COMMAND ----------

# Collect unique values into a list for each category
df_collected_set = df.groupBy("Category").agg(F.collect_set("Value").alias("UniqueValueList"))
df_collected_set.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) Using .rdd.flatMap()
# MAGIC
# MAGIC - This avoids explicit **list comprehension** and works with **Spark’s RDD**

# COMMAND ----------

values_list = df.select("status_type").rdd.flatMap(lambda x: x).collect()
print(values_list)

# COMMAND ----------

# MAGIC %md
# MAGIC - **.rdd** → converts the **DataFrame to an RDD**.
# MAGIC - **.flatMap(lambda x: x)** → flattens each **row** into a **single value**.
# MAGIC - **.collect()** → returns a **Python list**.