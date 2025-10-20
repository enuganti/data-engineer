# Databricks notebook source
# MAGIC %md
# MAGIC #### How to compare your DataFrame schema?
# MAGIC - **schema validation** snippet, used to **compare your DataFrame schema** (df.schema.fields) against an **expected schema** (expected_schema dictionary).

# COMMAND ----------

df = spark.read.csv("/Volumes/@azureadb/pyspark/unionby/company_level.csv", header=True, inferSchema=True)
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Validation Script**

# COMMAND ----------

expected_schema = {
    "start_date": "StringType()",
    "product_url": "StringType()",
    "category": "IntegerType()",
    "cloud_flatform": "IntegerType()",
    "default_group": "StringType()",
    "source_target": "StringType()",
    "product": "StringType()",
    "product_version": "StringType()",
    "session_id": "IntegerType()",
    "session_name": "StringType()",
    "status_name": "IntegerType()",
    "status_type": "StringType()",
    "sessions": "IntegerType()",
    "load datetime": "StringType()",
    "load date": "StringType()",
    "load time": "DoubleType()"
}

for field in df.schema.fields:
    expected_type = expected_schema.get(field.name)
    if expected_type:
        if str(field.dataType) == expected_type:
            print(f"✅ {field.name} → matches expected type {expected_type}")
        else:
            print(f"⚠️ {field.name} → expected {expected_type}, found {field.dataType}")
    else:
        print(f"ℹ️ Extra column: {field.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC - **df.schema.fields** gives a **list of StructField objects**, each representing a column.

# COMMAND ----------

# MAGIC %md
# MAGIC      df.schema.fields
# MAGIC
# MAGIC      [
# MAGIC       StructField('start_date', DateType(), True)
# MAGIC       StructField('product_url', StringType(), True)
# MAGIC       StructField('category', StringType(), True)
# MAGIC       StructField('default_group', StringType(), True)
# MAGIC       StructField('source_target', StringType(), True)
# MAGIC       StructField('cloud_flatform', StringType(), True)
# MAGIC       StructField('session_id', IntegerType(), True)
# MAGIC       StructField('session_name', StringType(), True)
# MAGIC       StructField('status_name', StringType(), True)
# MAGIC       StructField('status_type', StringType(), True)
# MAGIC       StructField('sessions', IntegerType(), True)
# MAGIC       StructField('product_id', IntegerType(), True)
# MAGIC       StructField('load datetime', StringType(), True)
# MAGIC      ]

# COMMAND ----------

# MAGIC %md
# MAGIC      for field in df.schema.fields:
# MAGIC          print(field)
# MAGIC      -------------------------------
# MAGIC      StructField('start_date', DateType(), True)
# MAGIC      StructField('product_url', StringType(), True)
# MAGIC      StructField('category', StringType(), True)
# MAGIC      StructField('default_group', StringType(), True)
# MAGIC      StructField('source_target', StringType(), True)
# MAGIC      StructField('cloud_flatform', StringType(), True)
# MAGIC      StructField('session_id', IntegerType(), True)
# MAGIC      StructField('session_name', StringType(), True)
# MAGIC      StructField('status_name', StringType(), True)
# MAGIC      StructField('status_type', StringType(), True)
# MAGIC      StructField('sessions', IntegerType(), True)
# MAGIC      StructField('product_id', IntegerType(), True)
# MAGIC      StructField('load datetime', StringType(), True)

# COMMAND ----------

for field in df.schema.fields:
    expected_type = expected_schema.get(field.name)
    print(expected_type)

# COMMAND ----------

# MAGIC %md
# MAGIC      expected_schema.get(field.name)
# MAGIC
# MAGIC      expected_schema.get(start_date) => StringType()
# MAGIC      expected_schema.get(product_url) => StringType()
# MAGIC      expected_schema.get(category) => IntegerType()
# MAGIC      expected_schema.get(default_group) => StringType()
# MAGIC      expected_schema.get(source_target) => StringType()
# MAGIC      expected_schema.get(cloud_flatform) => IntegerType()
# MAGIC      expected_schema.get(session_id) => IntegerType()
# MAGIC      expected_schema.get(session_name) => StringType()
# MAGIC      expected_schema.get(status_name) => IntegerType()
# MAGIC      expected_schema.get(status_type) => StringType()
# MAGIC      expected_schema.get(sessions) => IntegerType()
# MAGIC      expected_schema.get(product_id) => xxxxxx
# MAGIC      expected_schema.get(load datetime) => StringType()