# Databricks notebook source
# MAGIC %md
# MAGIC ##### json_tuple
# MAGIC - To **extract values** directly from a **JSON string** column **without converting** it into a **variant**.
# MAGIC - If you want to **extract multiple fields / separate columns** from a **flat JSON string**.
# MAGIC - It only works for **top-level fields (not nested JSON)**.
# MAGIC - It always returns **strings**, no matter the **original type**.
# MAGIC - All extracted fields are **strings** (you can **cast** later).
# MAGIC - It’s **schema-less** (you don’t need to define a schema).
# MAGIC
# MAGIC **Limitation:**
# MAGIC - Can’t parse **nested JSON** like **address.city**.
# MAGIC - For that → use **get_json_object, parse_json, or from_json**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Syntax
# MAGIC
# MAGIC      json_tuple(column, field1, field2, ..., fieldN)
# MAGIC
# MAGIC **column:**
# MAGIC - The **column** that contains the **JSON string**.
# MAGIC
# MAGIC **field1 ... fieldN:**
# MAGIC - The JSON **keys** you want to **extract**.
# MAGIC
# MAGIC **Returns:**
# MAGIC - **multiple string columns** (all extracted values are strings).

# COMMAND ----------

from pyspark.sql.functions import col, json_tuple

# COMMAND ----------

help(json_tuple)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Simple JSON

# COMMAND ----------

data = [
    ('{"id": 101, "name": "Subash", "age": 28, "city": "Bangalore", "price": 55000}',),
    ('{"id": 102, "name": "Dinesh", "age": 32, "city": "Chennai", "price": 25000}',),
    ('{"id": 103, "name": "Fathima", "age": 29, "city": "Nasik", "price": 35000}',),
    ('{"id": 104, "name": "Gopesh", "age": 35, "city": "Vizak", "price": 45000}',),
    ('{"id": 105, "name": "Sreeni", "age": 26, "city": "Hyderabad", "price": 26600}',),
    ('{"id": 106, "name": "Anitha", "age": 33, "city": "Salem", "price": 34400}',)
]

df = spark.createDataFrame(data, ["json_string"])

# Extract multiple fields
df_extracted = df.select('json_string',
    json_tuple("json_string", "id" ,"name", "age", "city", "price").alias("Id", "Name", "Age", "City", "Price")
)

df_extracted.display()

# COMMAND ----------

# DBTITLE 1,Convert data type: string to int
df_cast = df_extracted\
    .withColumn("ID", col("id").cast("int")) \
    .withColumn("AGE", col("age").cast("int")) \
    .withColumn("Price", col("Price").cast("int"))

df_cast.display()

# COMMAND ----------

# DBTITLE 1,Capitalize Columns
cols = ["id", "name", "age", "city", "price"]

df_ext_iter = df.select(
    json_tuple("json_string", *cols).alias(*[c.capitalize() for c in cols])
)

df_ext_iter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Nested JSON

# COMMAND ----------

data = [
    ('{"name": "Subash", "age": 28, "price": 55000, "address": {"city": "Bangalore", "zip": "560001"}}',),
    ('{"name": "Dinesh", "age": 32, "price": 65000, "address": {"city": "Chennai", "zip": "600001"}}',),
    ('{"name": "Fathima", "age": 29, "price": 59500, "address": {"city": "Nasik", "zip": "560001"}}',),
    ('{"name": "Gopesh", "age": 35, "price": 77000, "address": {"city": "Vizak", "zip": "600001"}}',),
    ('{"name": "Sreeni", "age": 26, "price": 89000, "address": {"city": "Hyderabad", "zip": "560001"}}',),
    ('{"name": "Anitha", "age": 33, "price": 44000, "address": {"city": "Salem", "zip": "600001"}}',)
]

df_nest = spark.createDataFrame(data, ["json_string"])
display(df_nest)

# COMMAND ----------

# Try to extract nested city from "address.city"
df_bad = df_nest.select(
    json_tuple("json_string", "name", "age", "price", "address.city", "address.zip").alias("Name", "Age", "Price" ,"City", "Zip")
)

display(df_bad)