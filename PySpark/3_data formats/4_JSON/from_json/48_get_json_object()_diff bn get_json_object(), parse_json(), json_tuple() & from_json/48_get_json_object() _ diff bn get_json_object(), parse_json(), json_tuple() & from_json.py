# Databricks notebook source
# MAGIC %md
# MAGIC ##### get_json_object
# MAGIC
# MAGIC - is used to **extract** a specific **JSON object or value** from a **JSON string** column within a DataFrame.
# MAGIC - Works directly on **JSON strings** (no need to **parse** into **struct** first).
# MAGIC - Useful when you need **only a few fields** from **raw JSON without schema**.

# COMMAND ----------

# MAGIC %md
# MAGIC      get_json_object()
# MAGIC      parse_json()
# MAGIC      json_tuple()
# MAGIC      from_json

# COMMAND ----------

# MAGIC %md
# MAGIC #### Syntax
# MAGIC
# MAGIC      get_json_object(col("json_column"), "$.path")
# MAGIC
# MAGIC      $ → root
# MAGIC      dot . → for nested objects
# MAGIC      [index] → for arrays
# MAGIC
# MAGIC |            JSON string                                                      | field ($.name) | field.subfield ($.details.city) | array[0] |
# MAGIC |-----------------------------------------------------------------------------|----------------|---------------------------------|------------|
# MAGIC | (1, '{"id": 1, "name": "Albert", "details": {"age": 30, "city": "Delhi"}}, "hobbies": ["reading", "sports", "music"]') |   Albert  |  Delhi  | reading  |
# MAGIC
# MAGIC - **$.field** → Top-level (root) field
# MAGIC - **$.field.subfield** → Nested field
# MAGIC - **$.array[0]** → First element in array
# MAGIC
# MAGIC **Return Value:**
# MAGIC - Returns **value** as a **string** (you must **cast** if you need **int, double,** etc.).
# MAGIC - The function returns a **Column object** containing the **extracted JSON string** of the specified **object or value**.
# MAGIC - If the **JSON string** is **invalid** or the **JSONPath does not match** any element, it returns **null**.

# COMMAND ----------

from pyspark.sql.functions import col, get_json_object, json_tuple, expr

# COMMAND ----------

help(get_json_object)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Simple JSON

# COMMAND ----------

data = [
    ('{"name": "Anand", "age": 28, "city": "Bangalore"}',),
    ('{"name": "Bibin", "age": 32, "city": "Chennai"}',),
    ('{"name": "Chandan", "age": 35, "city": "Hyderabad"}',),
    ('{"name": "Dora", "age": 37, "city": "Vadodara"}',),
    ('{"name": "Eswar", "age": 39, "city": "Pune"}',),
    ('{"name": "Sanjay", "age": 29, "city": "Cochin"}',)
]

df_sjson = spark.createDataFrame(data, ["json_string"])

df_simple_json = df_sjson.select('json_string',
    get_json_object(col("json_string"), "$.name").alias("Name"),
    get_json_object(col("json_string"), "$.age").alias("Age"),
    get_json_object(col("json_string"), "$.city").alias("City")
)

df_simple_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Nested JSON

# COMMAND ----------

data = [
    (1, '{"id": 1, "name": "Albert", "details": {"age": 30, "city": "Delhi"}}'),
    (2, '{"id": 2, "name": "Bobby", "details": {"age": 25, "city": "Baroda"}}'),
    (3, '{"id": 3, "name": "Swapna", "details": {"age": 35}}'),
    (4, '{"id": 4, "name": "David", "details": {"age": 25, "city": "Chennai"}}'),
    (5, '{"id": 5, "name": "Anand", "details": {"age": 25, "city": "Bangalore"}}'),
    (6, '{"id": 6, "name": "Baskar", "details": {"age": 25, "city": "Hyderabad"}}'),
]

df_kv = spark.createDataFrame(data, ["id", "json_data"])
display(df_kv)

# COMMAND ----------

# MAGIC %md
# MAGIC - **$** represents the **root of the JSON.**

# COMMAND ----------

df_kv.select(
    get_json_object(col("json_data"), "$.id").alias("Id"),
    get_json_object(col("json_data"), "$.name").alias("Name"),
    get_json_object(col("json_data"), "$.details.age").alias("Age"),
    get_json_object(col("json_data"), "$.details.city").alias("City")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Array in JSON

# COMMAND ----------

array_data = [
    ('{"id": 1, "hobbies": ["reading", "sports", "music"]}',),
    ('{"id": 2, "hobbies": ["travel", "cooking"]}',),
    ('{"id": 3, "hobbies": ["watching", "sports", "music"]}',),
    ('{"id": 4, "hobbies": ["cricket", "cooking"]}',),
    ('{"id": 5, "hobbies": ["football", "sports", "music"]}',),
    ('{"id": 6, "hobbies": ["hocky", "cooking"]}',)
]

df_array = spark.createDataFrame(array_data, ["json_string"])

df_array_result = df_array.select('json_string',
    get_json_object(col("json_string"), "$.id").alias("ID"),
    get_json_object(col("json_string"), "$.hobbies[0]").alias("Hobby1"),
    get_json_object(col("json_string"), "$.hobbies[1]").alias("Hobby2"),
    get_json_object(col("json_string"), "$.hobbies[2]").alias("Hobby3")
)

display(df_array_result)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Array of JSON string

# COMMAND ----------

json_str = """[{"Attr_INT":1, "ATTR_DOUBLE":10.101, "ATTR_DATE": "2021-01-01"},
{"Attr_INT":2, "ATTR_DOUBLE":20.201, "ATTR_DATE": "2022-02-11"},
{"Attr_INT":3, "ATTR_DOUBLE":30.301, "ATTR_DATE": "2023-04-21"},
{"Attr_INT":4, "ATTR_DOUBLE":40.401, "ATTR_DATE": "2024-05-15"},
{"Attr_INT":5, "ATTR_DOUBLE":50.501, "ATTR_DATE": "2025-03-25"}]"""

# Create a DataFrame
df_get = spark.createDataFrame([[1, json_str]], ['id', 'json_col'])
display(df_get)

# COMMAND ----------

# Extract JSON values
df_get_obj = df_get\
    .withColumn('ATTR_INT_0', get_json_object('json_col', '$[0].Attr_INT')) \
    .withColumn('ATTR_DOUBLE_1', get_json_object('json_col', '$[0].ATTR_DOUBLE')) \
    .withColumn('ATTR_DATE_2', get_json_object('json_col', '$[0].ATTR_DATE')) \
    .withColumn('ATTR_INT_3', get_json_object('json_col', '$[3].Attr_INT')) \
    .withColumn('ATTR_DOUBLE_3', get_json_object('json_col', '$[3].ATTR_DOUBLE')) \
    .withColumn('ATTR_DATE_3', get_json_object('json_col', '$[3].ATTR_DATE'))

display(df_get_obj)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Key Differences
# MAGIC
# MAGIC | Feature           | `parse_json`                                                 | `get_json_object`                                   |
# MAGIC | ----------------- | ------------------------------------------------------------ | --------------------------------------------------- |
# MAGIC | **What it does**  | Parses JSON into a **variant**                        | Extracts a **string value** by JSONPath             |
# MAGIC | **Return type**   | **Struct** (can expand into **multiple columns**)            | **String** (you must **cast** if you need **int, double**, etc.)  |
# MAGIC | **Schema**        | **Inferred** (flexible, can be used like a DataFrame column) | Not needed (**no schema applied**)                  |
# MAGIC | **Performance**   | Better for **multiple/nested** fields                        | Lightweight for extracting **1–2 fields**           |
# MAGIC | **Best use case** | When you need structured access to **many fields**           | When you just need a **few values quickly**         |
# MAGIC
# MAGIC | Method                | Input                | Output Type      | Pros                                            | Cons                         |
# MAGIC | --------------------- | -------------------- | ---------------- | ----------------------------------------------- | ---------------------------- |
# MAGIC | **get\_json\_object** | JSON string          | String           | Simple, lightweight                             | No schema, everything string |
# MAGIC | **parse\_json**       | JSON string          | variant          | Flexible, works without schema, preserves types | Schema not enforced          |
# MAGIC | **from\_json**        | JSON string + schema | Struct           | Strong typing, schema enforcement               | Must define schema           |
# MAGIC
# MAGIC | Function              | Input Type           | Output Type                                                                 | Schema Required?               | Supports Nested?             | Use Case                                                                |
# MAGIC | --------------------- | -------------------- | --------------------------------------------------------------------------- | ------------------------------ | ---------------------------- | ----------------------------------------------------------------------- |
# MAGIC | **`get_json_object`** | `string` (JSON text) | `string` (single value)                                                     | ❌ No                           | ✅ Yes (via JSON path)        | Extract one field from JSON using a JSONPath-like syntax                |
# MAGIC | **`parse_json`**      | `string` (JSON text) | `variant` (Spark internal JSON representation, like semi-structured object) | ❌ No                           | ✅ Yes                        | Directly parse JSON text into a flexible object (no schema enforcement) |
# MAGIC | **`from_json`**       | `string` (JSON text) | `struct`, `array`, or `map`                                                 | ✅ Yes (you must define schema) | ✅ Yes                        | Parse JSON into strongly-typed Spark SQL structs/arrays/maps            |
# MAGIC | **`json_tuple`**      | `string` (JSON text) | Multiple `string` columns                                                   | ❌ No                           | ❌ No (only top-level fields) | Quickly extract multiple top-level fields without schema                |
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, parse_json, get_json_object, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [
    ('{"name": "Kiran", "age": 28, "city": "Bangalore"}',),
    ('{"name": "Darshan", "age": 32, "city": "Chennai"}',),
    ('{"name": "Chetan", "age": 40, "city": "Hyderabad"}',),
    ('{"name": "Ramu", "age": 35, "city": "Varanasi"}',),
    ('{"name": "Priya", "age": 45, "city": "Amaravati"}',)
]

df_diff = spark.createDataFrame(data, ["json_string"])
display(df_diff)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Using get_json_object

# COMMAND ----------

df_getjson = df_diff.select(
    get_json_object(col("json_string"), "$.name").alias("Name"),
    get_json_object(col("json_string"), "$.age").alias("Age"),   # always string
    expr("CAST(get_json_object(json_string, '$.age') AS INT)").alias("Age_Int"),
    get_json_object(col("json_string"), "$.city").alias("City")
)
df_getjson.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Using parse_json

# COMMAND ----------

df_parse = df_diff.withColumn("json_data", parse_json(col("json_string")))

# Extract multiple fields
df_parse.select(
  expr("json_data:name::string").alias("Name"),
  expr("json_data:age::int").alias("Age"),       # keeps numeric type
  expr("json_data:city::string").alias("City")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Using from_json (explicit schema)

# COMMAND ----------

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

df_fromjson = df_diff.withColumn("json_data", from_json(col("json_string"), schema))

df_fromjson.select("json_string", "json_data", "json_data.*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **json_tuple**

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