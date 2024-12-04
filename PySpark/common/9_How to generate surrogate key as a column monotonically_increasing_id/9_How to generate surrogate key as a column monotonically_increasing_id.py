# Databricks notebook source
# MAGIC %md
# MAGIC **How to add Sequence generated surrogate key as a column in dataframe?**

# COMMAND ----------

# MAGIC %md
# MAGIC **Topics Covered**
# MAGIC
# MAGIC - monotonically_increasing_id
# MAGIC - Using MD5
# MAGIC - Using CRC32
# MAGIC - hash
# MAGIC - Using sha1
# MAGIC - Using sha2
# MAGIC - Using window function row_number()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) monotonically_increasing_id**
# MAGIC
# MAGIC - monotonically_increasing_id generates sequence or **surrogate key**.
# MAGIC - The monotonically_increasing_id function in Databricks is useful for generating **unique identifiers for rows in a DataFrame** and it generates a **column** with monotonically increasing **64-bit integers**. However, the IDs are not contiguous due to the way Spark operates in a distributed manner.
# MAGIC
# MAGIC **Key Characteristics**:
# MAGIC - **Monotonically Increasing:**
# MAGIC   - The values generated are guaranteed to be monotonically **increasing and unique**, but they are **not guaranteed to be consecutive**.
# MAGIC - **Distributed Processing:**
# MAGIC   - This function is optimized for distributed computing environments, such as Apache Spark, on which Databricks is built. The **IDs are unique across partitions** and can be used to identify rows uniquely.
# MAGIC - **Non-Consecutive IDs:**
# MAGIC   - Since the IDs are generated in a distributed manner, they are **not consecutive**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      monotonically_increasing_id()
# MAGIC **Arguments**
# MAGIC - This function takes no arguments.
# MAGIC
# MAGIC **Returns**
# MAGIC - BIGINT

# COMMAND ----------

from pyspark.sql.functions import col, lit, monotonically_increasing_id, spark_partition_id, concat, concat_ws
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 01: Consecutive numbers**

# COMMAND ----------

df = spark.range(10000)
display(df)

# COMMAND ----------

# Create a DataFrame with 1000 records
df_consec = spark.range(1000).withColumn("id", monotonically_increasing_id())\
                             .withColumn("partition_id", spark_partition_id())

# Display the DataFrame
display(df_consec)

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 02: Non-consecutive numbers**

# COMMAND ----------

# Create a DataFrame with 10 records
# df_Non_Consec = spark.range(10).withColumn("id", monotonically_increasing_id())\
#                                .withColumn("partition_id", spark_partition_id())

df_Non_Consec = spark.range(20).withColumn("id", monotonically_increasing_id())\
                               .withColumn("partition_id", spark_partition_id())

# Display the DataFrame
display(df_Non_Consec)

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/Emp_Hash-3.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

# Creating new column as partition_id using monotonically_increasing_id() function
df_surr = df.withColumn("ID_KEY", monotonically_increasing_id())
display(df_surr)

# COMMAND ----------

# MAGIC %md
# MAGIC **Setting a Custom Starting Point**

# COMMAND ----------

# Set a custom starting point for the IDs
start_id = 1

# Creating new column as partition_id using monotonically_increasing_id() function
df_surr = df.withColumn("ID_KEY", monotonically_increasing_id() + start_id)
display(df_surr)

# COMMAND ----------

# Set a custom starting point for the IDs
start_id = 1000

# Creating new column as partition_id using monotonically_increasing_id() function
df_surr = df.withColumn("ID_KEY", monotonically_increasing_id() + start_id)
display(df_surr)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Using CRC32**
# MAGIC
# MAGIC - Calculates the **cyclic redundancy check value (CRC32)** of a **binary column** and returns the value as a **bigint**.
# MAGIC - It generates duplicates for every 100k / 200k records.
# MAGIC - We should **not use CRC32** for **surrogate key** generation on **large tables**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      crc32(expr)
# MAGIC **Arguments**
# MAGIC - **expr:** A BINARY expression.
# MAGIC
# MAGIC **Returns**
# MAGIC - BIGINT

# COMMAND ----------

from pyspark.sql.functions import crc32, col

# Creating new column as partition_id using md5() function
df_CRC32 = df_surr.withColumn("CRC32_KEY", crc32(col("EMPNO").cast("string")))
display(df_CRC32)

# COMMAND ----------

from pyspark.sql.functions import concat, col, crc32, row_number
from pyspark.sql.window import Window

df_CRC32 = df_CRC32.withColumn("concat", concat(col("Sales"), col("Quantity"), col("Commodity"), col("Experience")))
df_CRC32 = df_CRC32.withColumn("CRC32_id", crc32(col("concat")))
df_CRC32 = df_CRC32.withColumn("duplicates", row_number().over(Window.partitionBy("CRC32_id").orderBy("CRC32_id")))
display(df_CRC32)

# COMMAND ----------

display(df_CRC32.filter("duplicates>1"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Using MD5**
# MAGIC - It generates duplicates for every 100k / 200k records.
# MAGIC - It not suggestionable to use while generating millions of records.

# COMMAND ----------

from pyspark.sql.functions import md5,col

# Creating new column as partition_id using md5() function
df_md5  = df_CRC32.withColumn("MD5_KEY", md5(col("EMPNO").cast("string")))
display(df_md5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **hash**
# MAGIC - Calculates the **hash code** of given **columns**, and returns the result as an **int column**.
# MAGIC - Used to **mask sensitive information**, e.g. Date of Birth, Social Security Number, I.P. Address, etc. Other times it may be needed to derive a repeatable ID/PrimaryKey column.

# COMMAND ----------

df_hash = df_md5.withColumn("hash", f.hash(col("EmpNo")))
display(df_hash)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Using sha1**

# COMMAND ----------

from pyspark.sql.functions import sha1

# Creating new column as partition_id using md5() function
df_sha1 = df_hash.withColumn("SHA1_KEY", sha1(concat(col("EMPNO"), col("Sales"), col("Quantity"))))
display(df_sha1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Using sha2**
# MAGIC
# MAGIC      df_CRC32.withColumn("SHA2_KEY", sha2(col("EMPNO").cast("string"), 256)) --> upto 200 million records
# MAGIC      df_CRC32.withColumn("SHA2_KEY", sha2(col("EMPNO").cast("string"), 512)) --> more than 200 million records

# COMMAND ----------

from pyspark.sql.functions import sha2

# Creating new column as partition_id using md5() function
df_sha2 = df_sha1.withColumn("SHA2_KEY", sha2(col("EMPNO").cast("string"),256))
display(df_sha2)

# COMMAND ----------

from pyspark.sql.functions import concat_ws, lit
df_sha2_lit = df_sha2.withColumn("SHA2_KEY_lit", concat_ws('_', lit('Salted'), col('EmpNo')))
df_sha2_lit = df_sha2_lit.withColumn("SHA2_KEY_concat", sha2(col("SHA2_KEY_lit").cast("string"),256))
display(df_sha2_lit)

# COMMAND ----------

df_sha2_lit_tr = df_sha1.withColumn("SHA2_KEY", sha2(col("EMPNO").cast("string"),200))
display(df_sha2_lit_tr)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Using window function row_number()**

# COMMAND ----------

from pyspark.sql.functions import sha2,row_number,lit
from pyspark.sql.window import Window

# Creating new column as partition_id using md5() function
df_row = df_sha2_lit.withColumn("ROW_NUMBER", row_number().over(Window.partitionBy(lit('')).orderBy(lit(''))))
display(df_row)
