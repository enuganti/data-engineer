# Databricks notebook source
# MAGIC %md
# MAGIC #### **to_avro**
# MAGIC
# MAGIC - to **encode** a column as **binary in Avro format**.
# MAGIC - **to_avro** is used to **convert** a Spark **DataFrame column** to **Avro binary format**.
# MAGIC - can be used to turn **structs into Avro records**. This method is particularly useful when you would like to **re-encode multiple columns into a single** one when writing data out to Kafka.

# COMMAND ----------

# MAGIC %md
# MAGIC      # Encode the column `name` in Avro format.
# MAGIC      output = df.select(to_avro("user.name").alias("value"))

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import to_avro, from_avro

# COMMAND ----------

# DBTITLE 1,Create a Sample DataFrame
data = [
    (1, "Arjun", 50000, "Nasik"),
    (2, "Bibin", 60000, "San Francisco"),
    (3, "Charu", 70000, "Los Angeles"),
    (4, "Anand", 55500, "Chennai"),
    (5, "Bhaskar", 65550, "Pondichery"),
    (6, "Chandan", 85500, "Bangalore"),
    (7, "Sai", 25000, "Hyderabad"),
    (8, "Rakesh", 35000, "Delhi"),
    (9, "Kiran", 95000, "Amaravati"),
    (10, "Rajini", 45000, "Dubai")
]

columns = ["id", "name", "salary", "city"]

df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Convert a Column to Avro Format**
# MAGIC - The Avro-encoded **binary data** is shown in a 
# MAGIC **readable format**.

# COMMAND ----------

# DBTITLE 1,Convert a Column to Avro Format
# to_avro to serialize the entire row into Avro format
df_avro_col = df.withColumn("encode_col", to_avro(df["name"]))
display(df_avro_col)

# COMMAND ----------

# MAGIC %md
# MAGIC **Convert Entire Row into Avro Format**
# MAGIC - Instead of just one column, you can convert the **entire row into Avro**.
# MAGIC - **struct("*")** => Converts **all columns** into a **single Avro-encoded structure**.

# COMMAND ----------

from pyspark.sql.functions import struct

df_avro_row = df.withColumn("encode_row", to_avro(struct("*")))
display(df_avro_row)

# COMMAND ----------

# DBTITLE 1,Writing Avro Data to a File
df_avro_row.write.format("avro").mode("overwrite").save("/FileStore/tables/avro/employee_avro_data")

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/avro/employee_avro_data

# COMMAND ----------

# DBTITLE 1,Reading Avro Data Back
df_read = spark.read.format("avro").load("/FileStore/tables/avro/employee_avro_data")
display(df_read)

# COMMAND ----------

df_read1 = spark.read.format("avro").load("dbfs:/FileStore/tables/avro/employee_avro_data/part-00000-tid-4540589104069149071-0300496f-0f56-4989-b549-d83183941916-154-1-c000.snappy.avro")
display(df_read1)

df_read2 = spark.read.format("avro").load("dbfs:/FileStore/tables/avro/employee_avro_data/part-00001-tid-4540589104069149071-0300496f-0f56-4989-b549-d83183941916-155-1-c000.snappy.avro")
display(df_read2)

df_read3 = spark.read.format("avro").load("dbfs:/FileStore/tables/avro/employee_avro_data/part-00002-tid-4540589104069149071-0300496f-0f56-4989-b549-d83183941916-156-1-c000.snappy.avro")
display(df_read3)

df_read4 = spark.read.format("avro").load("dbfs:/FileStore/tables/avro/employee_avro_data/part-00003-tid-4540589104069149071-0300496f-0f56-4989-b549-d83183941916-157-1-c000.snappy.avro")
display(df_read4)

df_read5 = spark.read.format("avro").load("dbfs:/FileStore/tables/avro/employee_avro_data/part-00004-tid-4540589104069149071-0300496f-0f56-4989-b549-d83183941916-158-1-c000.snappy.avro")
display(df_read5)

df_read6 = spark.read.format("avro").load("dbfs:/FileStore/tables/avro/employee_avro_data/part-00005-tid-4540589104069149071-0300496f-0f56-4989-b549-d83183941916-159-1-c000.snappy.avro")
display(df_read6)

df_read7 = spark.read.format("avro").load("dbfs:/FileStore/tables/avro/employee_avro_data/part-00006-tid-4540589104069149071-0300496f-0f56-4989-b549-d83183941916-160-1-c000.snappy.avro")
display(df_read7)

df_read8 = spark.read.format("avro").load("dbfs:/FileStore/tables/avro/employee_avro_data/part-00007-tid-4540589104069149071-0300496f-0f56-4989-b549-d83183941916-161-1-c000.snappy.avro")
display(df_read8)