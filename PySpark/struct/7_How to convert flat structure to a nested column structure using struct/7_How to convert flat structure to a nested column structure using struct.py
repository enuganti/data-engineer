# Databricks notebook source
# MAGIC %md
# MAGIC #### **Struct**
# MAGIC
# MAGIC - used to create a **new column** of type **StructType** by **combining multiple columns into a single struct column**.
# MAGIC
# MAGIC - you were given two columns and want to change column values from a **flat structure** to a **nested column structure**.
# MAGIC
# MAGIC - **Combining Columns:**
# MAGIC   - The struct function can **combine multiple columns** into a **single struct column**.
# MAGIC
# MAGIC - **Nested Structures:**
# MAGIC   - It allows for the creation of **nested structures**, which can be useful for **organizing related data**.
# MAGIC   - when you want to **group** related columns together into a **single column with a nested structure**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax:**
# MAGIC
# MAGIC      struct()
# MAGIC      struct(*columns) --> columns (list, set, str or column)

# COMMAND ----------

# MAGIC %md
# MAGIC      from pyspark.sql.functions import struct, col
# MAGIC  
# MAGIC      # Method 1:
# MAGIC      df = df.select(struct("f_name", "l_name").alias("name"))
# MAGIC  
# MAGIC      # Method 2:
# MAGIC      df = df.select(struct(["f_name", "l_name"]).alias("name"))
# MAGIC  
# MAGIC      # Method 3:
# MAGIC      df = df.select(struct([col("f_name"), col("l_name")]).alias("name"))
# MAGIC  
# MAGIC      # Method 4:
# MAGIC      columns = ("f_name", "l_name")
# MAGIC      df = df.select(struct(*columns).alias("name"))
# MAGIC
# MAGIC      df = df.withColumn("name", struct("f_name", "l_name"))

# COMMAND ----------

df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
display(df)
df.printSchema()

# COMMAND ----------

df.select(struct('age', 'name').alias("struct")).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **How to convert flat structure to a nested column structure**

# COMMAND ----------

from pyspark.sql.functions import struct, col, from_json, to_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as f

# COMMAND ----------

data = [(1, "Marry", "Terissa"), (2, "Kapil", "Sharma"), (3, "Niraj", "Gupta"), (4, "Amit", "Jain")]

schema = StructType([StructField("S.No", IntegerType(), False),
                     StructField("First_Name", StringType(), False),
                     StructField("Last_Name", StringType(), False)])

dff = spark.createDataFrame(data, schema=schema)
display(dff)

# COMMAND ----------

# using withColumn method
df1 = dff.withColumn("Name", struct("First_Name", "Last_Name"))
display(df1)

# COMMAND ----------

# using Select method 01
df2 = dff.select("*", struct("First_Name", "Last_Name").alias("Name"))
display(df2)

# COMMAND ----------

# using Select method 02
df3 = dff.select("*", struct(["First_Name", "Last_Name"]).alias("Name"))
display(df3)

# COMMAND ----------

# using Select method 03
df4 = dff.select("*", struct([col("First_Name"), col("Last_Name")]).alias("Name"))
display(df4)

# COMMAND ----------

# using Select method 04
cols = ("First_Name", "Last_Name")
df5 = dff.select("*", struct(*cols).alias("Name"))
display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **How to convert StructType column into StringType using to_json?**

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/StructType-5.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC      # Select all columns and structure them into a single column named 'Sales'
# MAGIC      df_stru = df_json.select(f.struct('*').alias('sales_msg')).distinct()
# MAGIC      display(df_stru.limit(10))
# MAGIC                                 (or)
# MAGIC      # Select all columns and structure them into a single column named 'Sales'
# MAGIC      sales_df_final = df.select(f.struct(
# MAGIC                                 f.col('Id'),
# MAGIC                                 f.col('Nick_Name'),
# MAGIC                                 f.col('First_Name'),
# MAGIC                                 f.col('Last_Name'),
# MAGIC                                 f.col('Type'),
# MAGIC                                 f.col('Age')
# MAGIC                                 ).alias('Sales')
# MAGIC                               )
# MAGIC
# MAGIC      display(sales_df_final)

# COMMAND ----------

# Select all columns and structure them into a single column named 'Sales'
sales_df_final = df.select(f.struct(
                     f.col('Id'),
                     f.col('Nick_Name'),
                     f.col('First_Name'),
                     f.col('Last_Name'),
                     f.col('Type'),
                     f.col('Age')
                     ).alias('Sales')
                     )

display(sales_df_final)

# COMMAND ----------

# Convert the 'Sales' column to JSON string
df_final = sales_df_final.withColumn('message', f.to_json('Sales'))
df_final.display()

# COMMAND ----------

df_schema = StructType([StructField('Id', IntegerType(), False),
                        StructField('Nick_Name', StringType(), False),
                        StructField('First_Name', StringType(), False),
                        StructField('Last_Name', StringType(), False),
                        StructField('Type', StringType(), False),
                        StructField('Age', IntegerType(), False)
                        ])

# COMMAND ----------

# Apply the from_json function on the JSON string column
df_final = df_final.select(f.from_json('message', df_schema).alias('kafka_message'))
display(df_final)
