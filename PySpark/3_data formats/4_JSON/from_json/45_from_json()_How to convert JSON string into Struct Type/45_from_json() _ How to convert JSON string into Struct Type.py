# Databricks notebook source
# MAGIC %md
# MAGIC #### from_json
# MAGIC
# MAGIC - Function is used to convert **JSON string** into **Struct type or Map type**.
# MAGIC - If the **string** is **unparseable**, it returns **null**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Pattern**
# MAGIC - Define **schema** (StructType / ArrayType)
# MAGIC - Apply **from_json(col("json_string"), schema)**
# MAGIC - Extract **struct/array** fields

# COMMAND ----------

# MAGIC %md
# MAGIC #### Syntax
# MAGIC
# MAGIC      from_json(col, schema, options={})
# MAGIC
# MAGIC
# MAGIC **options:**
# MAGIC **allowUnquotedFieldNames:**
# MAGIC - If set to true, allows unquoted field names in the JSON string. Default is false.
# MAGIC
# MAGIC **allowSingleQuotes:**
# MAGIC - If set to true, allows single quotes instead of double quotes in the JSON string. Default is true.
# MAGIC
# MAGIC **allowNumericLeadingZero:**
# MAGIC - If set to true, allows leading zeros in numeric values. Default is false.
# MAGIC
# MAGIC **allowBackslashEscapingAnyCharacter:**
# MAGIC - If set to true, allows backslash escaping any character in the JSON string. Default is false.
# MAGIC
# MAGIC **allowUnquotedControlChars:**
# MAGIC - If set to true, allows unquoted control characters in the JSON string. Default is false.
# MAGIC
# MAGIC **mode:**
# MAGIC - Specifies the parsing mode. It can be one of the following values:
# MAGIC
# MAGIC - **PERMISSIVE:**
# MAGIC   - Tries to parse all JSON records and sets fields to null if parsing fails. This is the default mode.
# MAGIC
# MAGIC - **DROPMALFORMED:**
# MAGIC   - Drops the whole row if any parsing error occurs.
# MAGIC
# MAGIC - **FAILFAST:**
# MAGIC   - Fails immediately if any parsing error occurs.

# COMMAND ----------

# MAGIC %md
# MAGIC **What’s json format?**
# MAGIC - The json format is meaning, **JavaScript Object Notation**.
# MAGIC
# MAGIC   - Data is in **name, value** pairs.
# MAGIC   - Data is separated by **commas**.
# MAGIC   - **Curly braces** hold **objects**.
# MAGIC   - **Square brackets** hold **arrays**.

# COMMAND ----------

from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Basic Usage with StructType
# MAGIC - Convert **JSON string** column into **struct**.

# COMMAND ----------

data = [
    ('{"name": "Albert", "age": 30, "city": "Bangalore"}',),
    ('{"name": "Bobby", "age": 25, "city": "Chennai"}',),
    ('{"name": "Swapna", "age": 35, "city": "Hyderabad"}',),
    ('{"name": "David", "age": 28, "city": "Cochin"}',),
    ('{"name": "Anand", "age": 33, "city": "Baroda"}',),
    ('{"name": "Baskar", "age": 29, "city": "Nasik"}',),
]

df_bjs = spark.createDataFrame(data, ["json_string"])

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

df_bjs_json = df_bjs.withColumn("json_data", from_json(col("json_string"), schema))
display(df_bjs_json)

# COMMAND ----------

# MAGIC %md
# MAGIC **Extract Struct Fields**

# COMMAND ----------

# MAGIC %md
# MAGIC      df_extracted = df_bjs_json.select(col("json_data.name").alias("name"),
# MAGIC                                        col("json_data.age").alias("age"),
# MAGIC                                        col("json_data.city").alias("city")
# MAGIC                                       )
# MAGIC                                       (or)
# MAGIC      df.select("json_data.*").display()

# COMMAND ----------

df_bjs_json.select("json_data.*").display()

# COMMAND ----------

df_extracted = df_bjs_json.select(
    col("json_data.name").alias("name"),
    col("json_data.age").alias("age"),
    col("json_data.city").alias("city")
)
df_extracted.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Nested JSON

# COMMAND ----------

# nested JSON rows
nested_data = [
    ('{"id":1, "Name":"Jyoti", "info":{"Experience":8, "city":"Bangalore","pin":560001}}',),
    ('{"id":2, "Name":"Sooraj", "info":{"Experience":10, "city":"Mysore","pin":570001}}',),
    ('{"id":3, "Name":"Swapna", "info":{"Experience":6, "city":"Chennai","pin":600001}}',),
    ('{"id":4, "Name":"Ravi", "info":{"Experience":5, "city":"Hyderabad","pin":500001}}',),
    ('{"id":5, "Name":"Anupam", "info":{"Experience":3, "city":"Mumbai","pin":400001}}',),
    ('{"id":6, "Name":"Kedar", "info":{"Experience":11, "city":"Delhi","pin":110001}}',),
    ('{"id":7, "Name":"Vivek", "info":{"Experience":9, "city":"Kolkata","pin":700001}}',),
    ('{"id":8, "Name":"Nagendar", "info":{"Experience":12, "city":"Pune","pin":411001}}',),
    ('{"id":9, "Name":"Madhu", "info":{"Experience":4, "city":"Ahmedabad","pin":380001}}',),
    ('{"id":10, "Name":"Sanjana", "info":{"Experience":2, "city":"Jaipur","pin":302001}}',),
]

# Create DataFrame with JSON string column
df_nested = spark.createDataFrame(nested_data, ["json_string"])

# Define schema for nested JSON
nested_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("info", StructType([
        StructField("Experience", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("pin", IntegerType(), True),
    ]))
])

# Parse JSON into struct
df_nested_parsed = df_nested.withColumn("json_data", from_json(col("json_string"), nested_schema))
display(df_nested_parsed)

# COMMAND ----------

df_nested_parsed.select(
    col("json_data.id").alias("Id"),
    col("json_data.Name").alias("Name"),
    col("json_data.info.Experience").alias("Experience"),
    col("json_data.info.city").alias("City"),
    col("json_data.info.pin").alias("Pin")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Array of JSON Objects

# COMMAND ----------

array_data = [
    ('[{"name":"Alice","age":25},{"name":"Bob","age":30}]',),
    ('[{"name":"Charlie","age":28},{"name":"David","age":35}]',),
    ('[{"name":"Eve","age":22},{"name":"Frank","age":40}]',),
    ('[{"name":"Grace","age":27},{"name":"Heidi","age":33}]',),
    ('[{"name":"Ivan","age":29},{"name":"Judy","age":31}]',),
    ('[{"name":"Kevin","age":26},{"name":"Laura","age":34}]',),
    ('[{"name":"Mallory","age":38},{"name":"Niaj","age":32}]',),
    ('[{"name":"Olivia","age":24},{"name":"Peggy","age":36}]',),
    ('[{"name":"Quentin","age":37},{"name":"Ruth","age":23}]',),
    ('[{"name":"Sybil","age":39},{"name":"Trent","age":28}]',),
]

df_array = spark.createDataFrame(array_data, ["json_string"])

array_schema = ArrayType(
    StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
)

# Parse JSON string into array<struct>
df_array_parsed = df_array.withColumn("json_data", from_json(col("json_string"), array_schema))
display(df_array_parsed)

# COMMAND ----------

# Explode array into individual rows
df_final = df_array_parsed.withColumn("person", explode(col("json_data"))) \
    .select("person",
        col("person.name").alias("name"),
        col("person.age").alias("age")
    )

display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Using from_json with Maps

# COMMAND ----------

from pyspark.sql.types import MapType

data_map = [
    ('{"id":1,"scores":{"math":90,"science":85}}',),
    ('{"id":2,"scores":{"math":75,"science":95}}',),
    ('{"id":3,"scores":{"math":80,"science":88}}',),
    ('{"id":4,"scores":{"math":85,"science":91}}',),
    ('{"id":5,"scores":{"math":79,"science":82}}',),
    ('{"id":6,"scores":{"math":83,"science":92}}',)
]

df_map = spark.createDataFrame(data_map, ["json_string"])

schema_map = StructType([
    StructField("id", IntegerType(), True),
    StructField("scores", MapType(StringType(), IntegerType()), True)
])

df_map_parsed = df_map.withColumn("json_data", from_json(col("json_string"), schema_map))
display(df_map_parsed)

# COMMAND ----------

df_map_parsed.select("json_data.id", "json_data.scores").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) nested_json_string

# COMMAND ----------

# JSON string
nested_json_string = '''{
    "name": "Alice",
    "age": 28,
    "address": {
        "street": "123 Main St",
        "city": "San Francisco",
        "zip": "94105"
    }
}'''

# Create DataFrame with JSON string
df_string = spark.createDataFrame([(nested_json_string,)], ["json_string"])

# Define schema for nested JSON
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True)
    ]), True)
])

# Parse JSON using from_json
df_parsed = df_string.withColumn("json_data", from_json(col("json_string"), schema))

print("Parsed DataFrame:")
display(df_parsed)

# Access nested fields
df_selected = df_parsed.select(
    col("json_data.name").alias("Name"),
    col("json_data.age").alias("Age"),
    col("json_data.address.street").alias("Street"),
    col("json_data.address.city").alias("City"),
    col("json_data.address.zip").alias("ZipCode")
)

print("Flattened DataFrame:")
display(df_selected)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6) Error Handling (Invalid JSON)
# MAGIC
# MAGIC - When **JSON strings** are **invalid**, from_json returns **null**.

# COMMAND ----------

# Define the JSON schema
json_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", StringType(), True)
])

data_invalid = [
    ('{"name":"Joseph","age":25}',),
    ('{"name":"Bharath","age":}',),  # invalid JSON
    ('{"name":"Jagadish","age":35}',),
    ('{"name":"Somu","age":28}',),
    ('{"name":"Dhamu","age":22}',),
    ('{"name":"Bharath","age":}',),  # invalid JSON
]
df_invalid = spark.createDataFrame(data_invalid, ["json_string"])

df_invalid_parsed = df_invalid.withColumn("json_data", from_json(col("json_string"), json_schema))
display(df_invalid_parsed)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7) Handling missing or corrupt data

# COMMAND ----------

# MAGIC %md
# MAGIC **PERMISSIVE**
# MAGIC
# MAGIC - **PERMISSIVE** is the **default mode** in Spark.
# MAGIC - Whether you **explicitly** set **{"mode": "PERMISSIVE"} or omit it**, the behavior is the **same**:
# MAGIC   - **Malformed rows** → parsed as **null**.
# MAGIC   - **Missing fields** → parsed as **null** in the **struct**.

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

# Sample DataFrame with JSON strings
df_dropMal = spark.createDataFrame([
    (1, '{"name": "Ramesh", "age": 30, "isStudent": false, "score": 9.5, "address": null}'),  # valid
    (2, '{"name": "Johny", "age": 25, "isStudent": true}'),                                   # valid but missing fields
    (3, '{"name": "Baskar", "age": 40, "isStudent": true')                                    # ❌ malformed JSON
], ["id", "data"])

# Define schema for the JSON column
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("isStudent", BooleanType(), True),
    StructField("score", DoubleType(), True),
    StructField("address", StringType(), True)
])

# Parse JSON using Permissive mode
df_permissive = df_dropMal.withColumn(
    "jsonData",
    from_json(col("data"), schema, {"mode": "PERMISSIVE"})
)

display(df_permissive)

# Flatten parsed struct into individual columns
df_flattened = df_permissive.select(
    col("id"),
    col("jsonData.name").alias("Name"),
    col("jsonData.age").alias("Age"),
    col("jsonData.isStudent").alias("IsStudent"),
    col("jsonData.score").alias("Score"),
    col("jsonData.address").alias("Address")
)

display(df_flattened)

# COMMAND ----------

# MAGIC %md
# MAGIC **FAILFAST**
# MAGIC
# MAGIC - **FAILFAST** means Spark will immediately **throw an error** if it encounters **malformed JSON**.
# MAGIC - If JSON is **valid** but **some fields are missing**, it will not fail — missing fields just become **null**.

# COMMAND ----------

# DBTITLE 1,FAILFAST
# Sample DataFrame with one malformed JSON row
df_failfast = spark.createDataFrame([
    (1, '{"name": "John", "age": 30, "isStudent": false, "score": 9.5, "address": null}'),  # valid
    (2, '{"name": "Jane", "age": 25, "isStudent": true}'),                                  # valid (missing fields -> nulls)
    (3, '{"name": "Bob", "age": 40, "isStudent": true')                                     # ❌ malformed (missing closing brace)
], ["id", "data"])


# Define schema for the JSON column
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("isStudent", BooleanType(), True),
    StructField("score", DoubleType(), True),
    StructField("address", StringType(), True)
])

# Parse JSON using FAILFAST mode (throws error on malformed JSON)
df_failfast_parsed = df_failfast.withColumn(
    "jsonData",
    from_json(col("data"), schema, {"mode": "FAILFAST"})
)

display(df_failfast_parsed)   # ❌ Will throw error on malformed JSON

# COMMAND ----------

# MAGIC %md
# MAGIC **DropMalformed**
# MAGIC - **Drops** the **whole row** if any **parsing error** occurs.
# MAGIC - DROPMALFORMED **drops malformed** JSON rows (i.e., sets parsed column to null).
# MAGIC - **Missing fields do not count** as malformed → they will just be **null** in the parsed struct.

# COMMAND ----------

# DBTITLE 1,DropMalformed
# Sample DataFrame with JSON strings
df_dropMal = spark.createDataFrame([
    (1, '{"name": "John", "age": 30, "isStudent": false, "score": 9.5, "address": null}'),  # valid
    (2, '{"name": "Jane", "age": 25, "isStudent": true}'),                                  # valid but missing fields
    (3, '{"name": "Bob", "age": 40, "isStudent": true')                                     # ❌ malformed JSON
], ["id", "data"])

display(df_dropMal)

# Define schema for the JSON column
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("isStudent", BooleanType(), True),
    StructField("score", DoubleType(), True),
    StructField("address", StringType(), True)
])

# Parse JSON using DropMalformed mode
df_dropMalformed = df_dropMal.withColumn(
    "jsonData",
    from_json(col("data"), schema, {"mode": "DROPMALFORMED"})
)

display(df_dropMalformed)

# Flatten parsed struct into individual columns
df_flattened = df_dropMalformed.select(
    col("id"),
    col("jsonData.name").alias("Name"),
    col("jsonData.age").alias("Age"),
    col("jsonData.isStudent").alias("IsStudent"),
    col("jsonData.score").alias("Score"),
    col("jsonData.address").alias("Address")
)

display(df_flattened)