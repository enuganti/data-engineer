# Databricks notebook source
# MAGIC %md
# MAGIC #### **PROBLEM STATEMENT**
# MAGIC
# MAGIC **How to extract portion of text after delimiter?**
# MAGIC  
# MAGIC  1) Read this **(, delimited) csv file**, create a dataframe
# MAGIC  2) Extract Column2 **(portion which is after the pipe)** along with column3
# MAGIC  3) Rename 2nd part of column2 to **ErrorCode** and column3 to **Count**
# MAGIC  
# MAGIC          Ex Output:
# MAGIC          ErrorCode, Count
# MAGIC          b3344002000,1.0
# MAGIC  4) Convert Column **Count** data type to **int**
# MAGIC  5) perform **Distinct** on dataframe created in step 4
# MAGIC  6) Add new column by name **ExecutionDate** having **today's date**.
# MAGIC  7) Add new column by name **Environment** having constant value **"Staging"**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Read this (, delimited) csv file, create a dataframe**

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/split.txt", header=True)
df.show()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Extract 'Column2' (portion which is after the pipe) along with 'column3'**

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01: split()**
# MAGIC
# MAGIC - Split
# MAGIC
# MAGIC       # Extract text after the delimiter
# MAGIC       df_with_extracted = df.withColumn("Extracted", split(df["column2"], r"\|").getItem(1))
# MAGIC                                            (or)
# MAGIC       # Extract text after the delimiter "|"
# MAGIC       df = df.withColumn("New_Column", split(df["column2"], r"\|")[1].alias("ExtractedText"))
# MAGIC                                           (or)
# MAGIC       df = df.withColumn("New_Column", split(df["column2"],'\|')[1].alias("ExtractedText"))
# MAGIC
# MAGIC **Method 02: regexp_extract()**
# MAGIC
# MAGIC - The regexp_extract() function extracts substrings based on a regular expression.
# MAGIC
# MAGIC       from pyspark.sql.functions import regexp_extract
# MAGIC
# MAGIC       # Extract text after the delimiter using regex
# MAGIC       df_with_extracted = df.withColumn("Extracted", regexp_extract(df["column2"], r"\| (.+)", 1))
# MAGIC
# MAGIC **Method 03: substring_index()**
# MAGIC
# MAGIC - The substring_index() function retrieves portions of a string relative to a delimiter.
# MAGIC      
# MAGIC       from pyspark.sql.functions import substring_index
# MAGIC
# MAGIC       # Extract text after the last occurrence of the delimiter
# MAGIC       df_with_extracted = df.withColumn("Extracted", substring_index(df["column2"], "|", -1).alias("Extracted"))
# MAGIC
# MAGIC **Method 04: expr()**
# MAGIC - expr()
# MAGIC
# MAGIC       from pyspark.sql.functions import expr
# MAGIC       
# MAGIC       # Extract text after the delimiter
# MAGIC       df_with_extracted = df.withColumn("Extracted", expr("split(column2, '\\|')[1]"))

# COMMAND ----------

from pyspark.sql.functions import split, size, col
from pyspark.sql.types import IntegerType

# COMMAND ----------

# Split function splits a string column into an array based on the specified delimiter
df_split = df.withColumn("New_Column", split(df["column2"],'\|'))
df_split.show(truncate=False)
df_split.printSchema()

# COMMAND ----------

df = df.withColumn("New_Column", split(df["column2"],'\|')[1])
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **split():**
# MAGIC - Splits **column2 into an array** using the **|** delimiter.
# MAGIC
# MAGIC **r"\|":**
# MAGIC - Escapes the | character (special in regex).
# MAGIC
# MAGIC **split(df["column2"], r"\|")[1]:**
# MAGIC - Selects the portion of text after the delimiter.
# MAGIC
# MAGIC **withColumn():**
# MAGIC - Creates a new column (ExtractedText) with the extracted portion.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Rename 2nd part of 'column2' to 'ErrorCode' and 'column3' to 'Count'**
# MAGIC **Ex: Output**
# MAGIC - ErrorCode, Count as b3344002000, 1.0

# COMMAND ----------

df = df.withColumnRenamed("New_Column", "ErrorCode")\
       .withColumnRenamed("column3", "Count")
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) Convert Column 'Count' data type to 'int'**

# COMMAND ----------

df = df.withColumn("Count", df["Count"].cast(IntegerType()))
df.show(truncate=False)
df.printSchema()
print("Number of Rows:", df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### **5) Perform Distinct on dataframe created in step 4**

# COMMAND ----------

df_distinct = df.distinct()
df_distinct.show()
print("Number of Rows", df_distinct.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### **6) Add new column by name 'ExecutionDate' having today's date**

# COMMAND ----------

from pyspark.sql.functions import current_date
df1 = df_distinct.withColumn("ExecutionDate", current_date())
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **7) Add new column by name 'Environment' having constant value "Staging"**

# COMMAND ----------

from pyspark.sql.functions import lit
df1 = df1.withColumn("Environment", lit("Staging"))
df1.show()
