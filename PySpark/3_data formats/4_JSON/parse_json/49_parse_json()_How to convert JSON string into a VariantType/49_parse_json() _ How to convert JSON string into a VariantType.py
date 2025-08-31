# Databricks notebook source
# MAGIC %md
# MAGIC #### parse_json
# MAGIC - For **newer versions** of **Spark (4.0 and above)**, the **pyspark.sql.functions.parse_json** function can be used to parse a **JSON string** into a **VariantType**.
# MAGIC
# MAGIC - Parses a column containing a **JSON string** into a **VariantType** (including **objects, arrays, strings, numbers, and null values**).
# MAGIC
# MAGIC - parse_json function returns an **error** if the **JSON string is malformed / not valid**. To return **NULL instead of an error**, use the **try_parse_json** function.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Syntax
# MAGIC
# MAGIC      pyspark.sql.functions.parse_json(col, schema=None, options={})
# MAGIC
# MAGIC **Parameters:**
# MAGIC
# MAGIC - **col** → Column containing the **JSON string** (usually a string column).
# MAGIC - **schema (optional)** →
# MAGIC   - **If provided** → the JSON string will be parsed into that schema.
# MAGIC   - **If omitted** → the JSON will be parsed into a VARIANT (loosely typed struct/map).
# MAGIC - **options (optional)** → Dictionary of options for parsing, e.g.
# MAGIC   - **"mode":** "PERMISSIVE" | "FAILFAST" | "DROPMALFORMED"
# MAGIC   - **"allowComments":** "true/false"
# MAGIC   - **"timestampFormat":** "yyyy-MM-dd'T'HH:mm:ss"
# MAGIC
# MAGIC **Returns:**
# MAGIC - A **VARIANT** value.

# COMMAND ----------

# MAGIC %md
# MAGIC **parse_json:**
# MAGIC - **does not take a schema**
# MAGIC - it only takes a **single argument** (the JSON column).
# MAGIC          
# MAGIC       TypeError: parse_json() takes 1 positional argument but 2 were given
# MAGIC
# MAGIC **✅ Correct usage:**
# MAGIC
# MAGIC - **parse_json(col)** → Parses a **JSON string** column into a loosely typed **variant/struct** (schema is inferred).
# MAGIC - **from_json(col, schema)** → Parses a **JSON string** column into a **user-defined schema**.

# COMMAND ----------

from pyspark.sql.functions import parse_json

# COMMAND ----------

help(parse_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Nested JSON
# MAGIC **a) Single nested JSON string**

# COMMAND ----------

nested_json_string = '''{"name": "Alice", "age": 28,
 "address": {"street": "123 Main St", "city": "San Francisco", "zip": "94105"}}'''

df_string = spark.createDataFrame([(nested_json_string,)], ["json_string"])
display(df_string)

# COMMAND ----------

# Parse string with VARIANT type
df_variant = df_string.withColumn("json_variant", parse_json(col("json_string")))
display(df_variant)

# COMMAND ----------

# Extract multiple fields
df_variant.select(
  expr("json_variant:age::int").alias("Age"),
  expr("json_variant:name::string").alias("Name"),
  expr("json_variant:address:city::string").alias("City"),
  expr("json_variant:address:street::string").alias("Street"),
  expr("json_variant:address:zip::int").alias("Zip")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Nested JSON string**

# COMMAND ----------

 # Sample nested JSON data
 data = [
     ('''{"name": "Alice", "age": 28,
          "address": {"street": "123 Main St", "city": "San Francisco", "zip": "94105"}}''',),

     ('''{"name": "Bob", "age": 32,
          "address": {"street": "456 Park Ave", "city": "New York", "zip": "10001"}}''',),

     ('''{"name": "Charlie", "age": 40,
          "address": {"street": "789 MG Road", "city": "Bangalore", "zip": "560001"}}''',),

     ('''{"name": "David", "age": 29,
          "address": {"street": "12 Residency Rd", "city": "Chennai", "zip": "600001"}}''',),

     ('''{"name": "Eva", "age": 35,
          "address": {"street": "90 Banjara Hills", "city": "Hyderabad", "zip": "500034"}}''',)
 ]

 # Create DataFrame with JSON strings
 df_json = spark.createDataFrame(data, ["json_string"])
 display(df_json)

# COMMAND ----------

# Parse JSON string into struct
df_parsed_01 = df_json.withColumn("json_data", parse_json(col("json_string")))
display(df_parsed_01)

# COMMAND ----------

# Extract multiple fields
df_parsed_01.select(
  expr("json_data:age::int").alias("Age"),
  expr("json_data:name::string").alias("Name"),
  expr("json_data:address:city::string").alias("City"),
  expr("json_data:address:street::string").alias("Street"),
  expr("json_data:address:zip::int").alias("Zip")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Simple JSON with key-value pairs

# COMMAND ----------

data = [
    ('{"name": "Albert", "age": 30, "city": "Bangalore"}',),
    ('{"name": "Bobby", "age": 25, "city": "Chennai"}',),
    ('{"name": "Swapna", "age": 35, "city": "Hyderabad"}',),
    ('{"name": "David", "age": 28, "city": "Cochin"}',),
    ('{"name": "Anand", "age": 33, "city": "Baroda"}',),
    ('{"name": "Baskar", "age": 29, "city": "Nasik"}',),
]

df_kv = spark.createDataFrame(data, ["json_string"])

# Convert JSON string into struct
df_parsed = df_kv.withColumn("json_data", parse_json(col("json_string")))
display(df_parsed)

# COMMAND ----------

# Extract multiple fields
df_result_expr = df_parsed.select(
    expr("json_data:age::int").alias("Age"),
    expr("json_data:name::string").alias("Name"),
    expr("json_data:city::string").alias("City")
)

display(df_result_expr)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL

# COMMAND ----------

# DBTITLE 1,1) JSON string
# MAGIC %sql
# MAGIC -- Convert a simple JSON object to VARIANT
# MAGIC SELECT parse_json('{"key": 123, "data": [4, 5, "str"]}') AS variant_data;

# COMMAND ----------

# DBTITLE 1,2) Array
# MAGIC %sql
# MAGIC -- Convert a JSON array to VARIANT
# MAGIC SELECT parse_json('[1, 2, 3, {"nested_key": "nested_value"}]') AS variant_array;

# COMMAND ----------

# DBTITLE 1,3) Scalar
# MAGIC %sql
# MAGIC -- Convert a simple scalar value to VARIANT
# MAGIC SELECT parse_json('"A simple string"') AS variant_string;

# COMMAND ----------

# DBTITLE 1,4) integer
# MAGIC %sql
# MAGIC -- Convert a numeric value to VARIANT
# MAGIC SELECT parse_json('12345') AS variant_number;

# COMMAND ----------

# DBTITLE 1,5) Null's
# MAGIC %sql
# MAGIC -- Handling null values
# MAGIC SELECT parse_json(null) AS variant_null;