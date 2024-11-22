# Databricks notebook source
# MAGIC %md
# MAGIC #### **How to read csv, json, parquet, avro & orc formats in databricks SQL editor**
# MAGIC
# MAGIC - To read a CSV file in Databricks SQL, you can use the **read_files** table-valued function.
# MAGIC
# MAGIC - This function is available in **Databricks Runtime 13.3 LTS and above**.
# MAGIC
# MAGIC - Reads files under a provided location and returns the data in tabular form.
# MAGIC
# MAGIC - Supports reading **JSON, CSV, XML, TEXT, BINARYFILE, PARQUET, AVRO, and ORC** file formats. Can detect the file format automatically and infer a unified schema across all files.
# MAGIC
# MAGIC - read_files can read an **individual file** or read files under a provided **directory**.
# MAGIC
# MAGIC - read_files discovers **all files** under the provided **directory recursively** unless a **glob** is provided, which instructs read_files to recurse into a specific directory pattern.

# COMMAND ----------

# MAGIC %md
# MAGIC #### `Read csv file`
# MAGIC      SELECT * 
# MAGIC      FROM read_files(
# MAGIC        '/path/to/your/csvfile.csv',
# MAGIC        format => 'csv',
# MAGIC        header => true,
# MAGIC        mode => 'FAILFAST'
# MAGIC      )
# MAGIC
# MAGIC #### `Read JSON file`
# MAGIC      SELECT * 
# MAGIC      FROM read_files(
# MAGIC        '/path/to/your/jsonfile.json',
# MAGIC        format => 'json',
# MAGIC        mode => 'FAILFAST'
# MAGIC      )
# MAGIC
# MAGIC #### `Read parquet file`
# MAGIC      SELECT * 
# MAGIC      FROM read_files(
# MAGIC        '/path/to/your/parquetfile.parquet',
# MAGIC        format => 'parquet',
# MAGIC        mode => 'FAILFAST'
# MAGIC      )
# MAGIC     
# MAGIC #### `Read avro file`
# MAGIC      SELECT * 
# MAGIC      FROM read_files(
# MAGIC        '/path/to/your/avrofile.avro',
# MAGIC        format => 'avro',
# MAGIC        mode => 'FAILFAST'
# MAGIC      )
# MAGIC
# MAGIC #### `Read ORC file`
# MAGIC      SELECT * 
# MAGIC      FROM read_files(
# MAGIC        '/path/to/your/avrofile.avro',
# MAGIC        format => 'orc',
# MAGIC        mode => 'FAILFAST'
# MAGIC      )

# COMMAND ----------

# MAGIC %md
# MAGIC - **'/path/to/your/csvfile.csv'** is the path to your CSV file.
# MAGIC - **format => 'csv'** specifies that the file format is CSV.
# MAGIC - **header => true** indicates that the CSV file has a header row.
# MAGIC - **mode => 'FAILFAST'** ensures that the query fails if any **malformed lines** are encountered.

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

# MAGIC %md
# MAGIC      %fs ls dbfs:/databricks-datasets/
# MAGIC      %fs ls dbfs:/databricks-datasets/nyctaxi/
# MAGIC      %fs ls dbfs:/databricks-datasets/nyctaxi/tables/
# MAGIC      %fs ls dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) How to read csv file SQL?**

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**
# MAGIC
# MAGIC      SELECT * FROM csv.`dbfs:/FileStore/tables/to_date.csv`
# MAGIC
# MAGIC **Method 02**
# MAGIC
# MAGIC      SELECT * 
# MAGIC      FROM read_files(
# MAGIC        'dbfs:/FileStore/tables/to_date.csv',
# MAGIC        format => 'csv',
# MAGIC        header => true,
# MAGIC        mode => 'FAILFAST'
# MAGIC      )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`dbfs:/FileStore/tables/to_date.csv`
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Read csv file
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC   'dbfs:/FileStore/tables/to_date.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   mode => 'FAILFAST'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) How to read JSON file SQL?**

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**
# MAGIC
# MAGIC      SELECT * FROM json.`dbfs:/FileStore/tables/multiline.json`
# MAGIC
# MAGIC **Method 02**
# MAGIC
# MAGIC      SELECT * 
# MAGIC      FROM read_files(
# MAGIC        'dbfs:/FileStore/tables/multiline.json',
# MAGIC        format => 'json',
# MAGIC        header => true,
# MAGIC        mode => 'FAILFAST'
# MAGIC      )

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/iot/

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`dbfs:/databricks-datasets/iot/iot_devices.json`
# MAGIC LIMIT 8

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC   'dbfs:/databricks-datasets/iot/iot_devices.json',
# MAGIC   format => 'json',
# MAGIC   mode => 'FAILFAST'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`dbfs:/FileStore/tables/multiline.json`

# COMMAND ----------

# DBTITLE 1,Read json file
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC   'dbfs:/FileStore/tables/multiline.json',
# MAGIC   format => 'json',
# MAGIC   mode => 'FAILFAST'
# MAGIC )

# COMMAND ----------

# Path to your JSON file
json_file_path = "dbfs:/FileStore/tables/multiline.json"

# Read the JSON file into a DataFrame
df = spark.read.format("json")\
  .option('header', True)\
  .option('inferSchema', True)\
  .option('multiLine', True)\
  .load(json_file_path)

display(df)

# COMMAND ----------

# Create a temporary view
df.createOrReplaceTempView("json_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json_view

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) How to read AVRO file SQL?**

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**
# MAGIC
# MAGIC      SELECT * FROM avro.`/FileStore/tables/multiline_nested_01.avro`
# MAGIC
# MAGIC **Method 02**
# MAGIC
# MAGIC      SELECT * 
# MAGIC      FROM read_files(
# MAGIC        '/FileStore/tables/multiline_nested_01.avro',
# MAGIC        format => 'avro',
# MAGIC        mode => 'FAILFAST'
# MAGIC      )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM avro.`/FileStore/tables/multiline_nested_01.avro`
# MAGIC LIMIT 8

# COMMAND ----------

# DBTITLE 1,read avro file
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC   '/FileStore/tables/multiline_nested_01.avro',
# MAGIC   format => 'avro',
# MAGIC   mode => 'FAILFAST'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) How to read parquet file SQL?**

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**
# MAGIC
# MAGIC      SELECT * FROM parquet.`/FileStore/tables/multiline_nested_parquet_01.parquet`
# MAGIC
# MAGIC **Method 02**
# MAGIC
# MAGIC      SELECT * 
# MAGIC      FROM read_files(
# MAGIC        '/FileStore/tables/multiline_nested_parquet_01.parquet',
# MAGIC        format => 'parquet',
# MAGIC        mode => 'FAILFAST'
# MAGIC      )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`/FileStore/tables/multiline_nested_parquet_01.parquet`
# MAGIC LIMIT 8

# COMMAND ----------

# DBTITLE 1,read parquet file
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC   '/FileStore/tables/multiline_nested_parquet_01.parquet',
# MAGIC   format => 'parquet',
# MAGIC   mode => 'FAILFAST'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### **5) How to read ORC file SQL?**

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**
# MAGIC
# MAGIC      SELECT * FROM orc.`/FileStore/tables/multiline_nested_parquet_01.parquet`
# MAGIC
# MAGIC **Method 02**
# MAGIC
# MAGIC      SELECT * 
# MAGIC      FROM read_files(
# MAGIC        '/FileStore/tables/multiline_nested_parquet_01.parquet',
# MAGIC        format => 'orc',
# MAGIC        header => true,
# MAGIC        mode => 'FAILFAST'
# MAGIC      )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC   '/FileStore/tables/multiline_nested_01.orc',
# MAGIC   format => 'orc',
# MAGIC   header => true,
# MAGIC   mode => 'FAILFAST'
# MAGIC )

# COMMAND ----------

# Path to your JSON file
json_file_path = "/FileStore/tables/multiline_nested_01.orc"

# Read the JSON file into a DataFrame
df = spark.read.format("orc")\
  .option('header', True)\
  .load(json_file_path)

display(df)

# COMMAND ----------

# Create a temporary view
df.createOrReplaceTempView("orc_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orc_view

# COMMAND ----------

# MAGIC %md
# MAGIC #### **6) How to read delta file SQL?**

# COMMAND ----------

# MAGIC %md
# MAGIC      %fs ls dbfs:/databricks-datasets/
# MAGIC      %fs ls dbfs:/databricks-datasets/nyctaxi/
# MAGIC      %fs ls dbfs:/databricks-datasets/nyctaxi/tables/
# MAGIC      %fs ls dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM delta.`dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nytrips AS
# MAGIC SELECT * FROM delta.`dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/`
# MAGIC LIMIT 8

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nytrips

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED nytrips
