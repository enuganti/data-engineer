# Databricks notebook source
# MAGIC %md
# MAGIC **Method 01**

# COMMAND ----------

data = [15, 25, 36, 44, 57, 65, 89, 95, 9]

df4 = spark.createDataFrame([(x,) for x in data], ["Numbers"])
display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC **✅ Why use (x,) and not just [x for x in data]?**
# MAGIC
# MAGIC - **[(x,) for x in data]** creates a **list of 1-element tuples**:
# MAGIC
# MAGIC       [(15,), (25,), (36,), ..., (9,)]
# MAGIC
# MAGIC   - Each item is a **tuple with one element**, which maps correctly to the **one-column schema ["Numbers"]**. PySpark sees **each tuple as a row**.
# MAGIC   
# MAGIC   - **createDataFrame()** expects each element of the input **list to be a row**, which means a **tuple or list** representing the values in each column.
# MAGIC
# MAGIC - So, when you write:
# MAGIC
# MAGIC       [(x,) for x in data]
# MAGIC
# MAGIC   - **[(x,)]** creates a **list of tuples**, each containing a **single value** — this is what PySpark expects when you give it a schema like ["Numbers"]
# MAGIC
# MAGIC   - **each row** has **1 value** (for the column Numbers), so create it as a **tuple of one element**.

# COMMAND ----------

df4 = spark.createDataFrame([x for x in data], ["Numbers"])
display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC **❌ What happens with [x for x in data]?**
# MAGIC
# MAGIC - **[x for x in data]** produces
# MAGIC
# MAGIC       [15, 25, 36, 44, 57, 65, 89, 95, 9]
# MAGIC
# MAGIC   - This is a **list of integers, not a list of rows/tuples**.
# MAGIC   - These are **integers, not tuples or rows**. Spark doesn't know how to treat an **int as a row**. It expects something like **(15,) or [15]** to match with the column definition **["Numbers"]**.
# MAGIC - If you try:
# MAGIC
# MAGIC       df = spark.createDataFrame([15, 25, 36], ["Numbers"])
# MAGIC
# MAGIC - You'll get an **error** like:
# MAGIC
# MAGIC       TypeError: StructType can only be created from list or tuple, got <class 'int'>
# MAGIC       TypeError: StructType can not be applied to an int
# MAGIC
# MAGIC   - Because Spark tries to interpret **15 as a row**, and it **can't match** it to the **schema**.

# COMMAND ----------

# MAGIC %md
# MAGIC **✅ Method 02: Alternate using List**
# MAGIC
# MAGIC - This works too, because each **[x]** is a **one-element list**, Spark can treat this as a **single-row** entry.

# COMMAND ----------

df4 = spark.createDataFrame([[x] for x in data], ["Numbers"])
display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC **✅ Method 03: Alternative using Row**

# COMMAND ----------

from pyspark.sql import Row

df = spark.createDataFrame([Row(Numbers=x) for x in data])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Summary:**
# MAGIC | Expression	| Works?	 | Reason |
# MAGIC |-------------|----------|--------|
# MAGIC | [(x,) for x in data]	| ✅	| Tuple per row (1-element row). Spark accepts this.|
# MAGIC | [[x] for x in data]	| ✅	| List per row. Spark accepts this as well. |
# MAGIC | [x for x in data]	| ❌	| Just a list of integers — Spark can't treat plain ints as row data. |

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 04: List of tuples (single element)**

# COMMAND ----------

# Create a sample DataFrame
data = [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)]
df = spark.createDataFrame(data, ["id"])
display(df)