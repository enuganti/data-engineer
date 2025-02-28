# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Write DataFrame to Avro & Load Avro files into DataFrame**

# COMMAND ----------

# MAGIC %md
# MAGIC **a) Without define schema**

# COMMAND ----------

# DBTITLE 1,sample dataframe
# Sample DataFrame
data = [(1, "kiran", 50000, "Chennai", 2012, 8, "Batman", 9.8),
        (2, "Jayesh", 60000, "Bangalore", 2012, 8, "Hero", 8.7),
        (3, "Mithun", 55000, "Hyderabad", 2012, 7, "Robot", 5.5),
        (4, "Muthu", 25000, "Chennai", 2011, 7, "Git", 2.0),
        (5, "Nirmal", 35000, "Nasik", 2011, 8, "Azure", 2.5),
        (6, "Naresh", 65000, "Hyderabad", 2012, 7, "ADF", 6.5),
        (7, "Kamal", 85000, "Chennai", 2011, 7, "ADB", 3.8),
        (8, "Kiran", 95000, "Nasik", 2011, 8, "Azure", 4.5)]
        
columns = ["id", "name", "salary", "city", "year", "month", "title", "rating"]

df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

# DBTITLE 1,save as avro with partition by
# Write DataFrame to Avro
df.write.format("avro").mode("overwrite").save("/FileStore/tables/avro/Employee_data")

# Write DataFrame to Avro with partition by "city"
df.write.format("avro").mode("overwrite").partitionBy("city").save("/FileStore/tables/avro/city_partitioned")

# Write DataFrame to Avro with partition by "year", "month"
df.write.format("avro").mode("overwrite").partitionBy("year", "month").save("/FileStore/tables/avro/year_month_partitioned")

# COMMAND ----------

# MAGIC %md
# MAGIC **Supported Avro Compression Codecs in PySpark**
# MAGIC - "uncompressed" (for no compression)
# MAGIC - "snappy" (default and recommended)
# MAGIC - "deflate"
# MAGIC - "bzip2"
# MAGIC - "xz"
# MAGIC - **"lz4" and "gzip"** are **NOT supported** for Avro in PySpark.

# COMMAND ----------

# DBTITLE 1,save as avro with different compression
# Write DataFrame to Avro with compression "snappy"
df.write.format("avro").mode("overwrite")\
    .option("compression", "snappy").save("/FileStore/tables/avro/Employee_data_snappy")

# Write DataFrame to Avro with compression "uncompressed"
df.write.format("avro").mode("overwrite")\
    .option("compression", "uncompressed").save("/FileStore/tables/avro/Employee_data_uncompressed")

# Write DataFrame to Avro with compression "deflate"
df.write.format("avro").mode("overwrite")\
    .option("compression", "deflate").save("/FileStore/tables/avro/Employee_data_deflate")

# Write DataFrame to Avro with compression "bzip2"
df.write.format("avro").mode("overwrite")\
    .option("compression", "bzip2").save("/FileStore/tables/avro/Employee_data_bzip2")

# Write DataFrame to Avro with compression "xz"
df.write.format("avro").mode("overwrite")\
    .option("compression", "xz").save("/FileStore/tables/avro/Employee_data_xz")

# COMMAND ----------

# MAGIC %md
# MAGIC      %fs ls dbfs:/FileStore/tables/avro/
# MAGIC                       (or)
# MAGIC      display(dbutils.fs.ls("/FileStore/tables/avro/"))

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/avro/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/avro/Employee_data/

# COMMAND ----------

# Read Avro file with schema
df123 = spark.read.format("avro").load("dbfs:/FileStore/tables/avro/Employee_data/part-00000-tid-3582650581164422351-7fbbe85c-9c76-46a7-b7c9-947e04b72b1c-56-1-c000.snappy.avro")

# Show DataFrame
display(df123)

# COMMAND ----------

# DBTITLE 1,read avro file
# Read Avro file with schema
df1 = spark.read.format("avro").load("dbfs:/FileStore/tables/avro/Employee_data/")

# Show DataFrame
display(df1)

# COMMAND ----------

# DBTITLE 1,read avro file with schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DoubleType

# Define schema
schema = StructType([StructField("id", LongType(), True),
                     StructField("name", StringType(), True),
                     StructField("salary", LongType(), True),
                     StructField("city", StringType(), True),
                     StructField("year", LongType(), True),
                     StructField("month", LongType(), True),
                     StructField("title", StringType(), True),
                     StructField("rating", DoubleType(), True)
                    ])

# Read Avro file with schema
df1 = spark.read.format("avro").schema(schema).load("dbfs:/FileStore/tables/avro/Employee_data/")

# Show DataFrame
display(df1)

# COMMAND ----------

# DBTITLE 1,read avro file (Compression)
# Handling Compression
# Read Avro file with specified compression code
df_snappy = spark.read.format("avro")\
    .option("compression", "snappy").load("/FileStore/tables/avro/Employee_data_snappy")

# Show DataFrame
display(df_snappy)

# Read Avro file with specified compression code "uncompressed"
df_uncompressed = spark.read.format("avro")\
    .option("compression", "uncompressed").load("/FileStore/tables/avro/Employee_data_uncompressed")

# Show DataFrame
display(df_uncompressed)

# Read Avro file with specified compression code "deflate"
df_deflate = spark.read.format("avro")\
    .option("compression", "deflate").load("/FileStore/tables/avro/Employee_data_deflate")

# Show DataFrame
display(df_deflate)

# Read Avro file with specified compression code "bzip2"
df_bzip2 = spark.read.format("avro")\
    .option("compression", "bzip2").load("/FileStore/tables/avro/Employee_data_bzip2")

# Show DataFrame
display(df_bzip2)

# Read Avro file with specified compression code "xz"
df_xz = spark.read.format("avro")\
    .option("compression", "xz").load("/FileStore/tables/avro/Employee_data_xz")

# Show DataFrame
display(df_xz)

# COMMAND ----------

# MAGIC %md
# MAGIC **b) With schema**

# COMMAND ----------

# DBTITLE 1,sample dataframe with schema
# Sample DataFrame
data = [(1, "kiran", 50000, "Chennai", 2012, 8, "Batman", 9.8),
        (2, "Jayesh", 60000, "Bangalore", 2012, 8, "Hero", 8.7),
        (3, "Mithun", 55000, "Hyderabad", 2012, 7, "Robot", 5.5),
        (4, "Muthu", 25000, "Chennai", 2011, 7, "Git", 2.0),
        (5, "Nirmal", 35000, "Nasik", 2011, 8, "Azure", 2.5),
        (6, "Naresh", 65000, "Hyderabad", 2012, 7, "ADF", 6.5),
        (7, "Kamal", 85000, "Chennai", 2011, 7, "ADB", 3.8),
        (8, "Kiran", 95000, "Nasik", 2011, 8, "Azure", 4.5)]
        
# Define schema
schema = StructType([StructField("id", IntegerType(), True),
                     StructField("name", StringType(), True),
                     StructField("salary", IntegerType(), True),
                     StructField("city", StringType(), True),
                     StructField("year", IntegerType(), True),
                     StructField("month", IntegerType(), True),
                     StructField("title", StringType(), True),
                     StructField("rating", DoubleType(), True)
                    ])

df_schema = spark.createDataFrame(data, schema)
display(df_schema)

# COMMAND ----------

# Write DataFrame to Avro
df_schema.write.format("avro").mode("overwrite").save("/FileStore/tables/avro/Employee_data_schema")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/avro/

# COMMAND ----------

# Read Avro file with schema
df1_schema = spark.read.format("avro").load("dbfs:/FileStore/tables/avro/Employee_data_schema/")

# Show DataFrame
display(df1_schema)

# COMMAND ----------

# Define schema
schema = StructType([StructField("id", IntegerType(), True),
                     StructField("name", StringType(), True),
                     StructField("salary", IntegerType(), True),
                     StructField("city", StringType(), True),
                     StructField("year", IntegerType(), True),
                     StructField("month", IntegerType(), True),
                     StructField("title", StringType(), True),
                     StructField("rating", DoubleType(), True)
                    ])

# Read Avro file with schema
df2_schema = spark.read.format("avro").schema(schema).load("dbfs:/FileStore/tables/avro/Employee_data_schema/")

# Show DataFrame
display(df2_schema)

# COMMAND ----------

df2_schema.filter("city == 'Chennai'").display()

# COMMAND ----------

df2_schema.filter("year == 2011").display()

# COMMAND ----------

from pyspark.sql.functions import concat_ws
df2_schema = df2_schema.withColumn("full_name", concat_ws(", ", "name", "title"))
display(df2_schema)

# COMMAND ----------

# Write DataFrame to Avro
df2_schema.write.format("avro").mode("overwrite").save("dbfs:/FileStore/tables/avro/Employee_data_schema/")

# COMMAND ----------

# Read Avro file with schema
df3_schema = spark.read.format("avro").load("dbfs:/FileStore/tables/avro/Employee_data_schema/")

# Show DataFrame
display(df3_schema)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/avro/Employee_data_schema/

# COMMAND ----------

spark.read.format("avro").load("dbfs:/FileStore/tables/avro/Employee_data_schema/part-00000-tid-7958917076368329786-dbbb74aa-1447-4dd9-8b14-13b8ff144aba-357-1-c000.snappy.avro").display()

spark.read.format("avro").load("dbfs:/FileStore/tables/avro/Employee_data_schema/part-00001-tid-7958917076368329786-dbbb74aa-1447-4dd9-8b14-13b8ff144aba-358-1-c000.snappy.avro").display()

spark.read.format("avro").load("dbfs:/FileStore/tables/avro/Employee_data_schema/part-00002-tid-7958917076368329786-dbbb74aa-1447-4dd9-8b14-13b8ff144aba-359-1-c000.snappy.avro").display()

spark.read.format("avro").load("dbfs:/FileStore/tables/avro/Employee_data_schema/part-00003-tid-7958917076368329786-dbbb74aa-1447-4dd9-8b14-13b8ff144aba-360-1-c000.snappy.avro").display()

spark.read.format("avro").load("dbfs:/FileStore/tables/avro/Employee_data_schema/part-00004-tid-7958917076368329786-dbbb74aa-1447-4dd9-8b14-13b8ff144aba-361-1-c000.snappy.avro").display()

spark.read.format("avro").load("dbfs:/FileStore/tables/avro/Employee_data_schema/part-00005-tid-7958917076368329786-dbbb74aa-1447-4dd9-8b14-13b8ff144aba-362-1-c000.snappy.avro").display()

spark.read.format("avro").load("dbfs:/FileStore/tables/avro/Employee_data_schema/part-00006-tid-7958917076368329786-dbbb74aa-1447-4dd9-8b14-13b8ff144aba-363-1-c000.snappy.avro").display()

spark.read.format("avro").load("dbfs:/FileStore/tables/avro/Employee_data_schema/part-00007-tid-7958917076368329786-dbbb74aa-1447-4dd9-8b14-13b8ff144aba-364-1-c000.snappy.avro").display()
