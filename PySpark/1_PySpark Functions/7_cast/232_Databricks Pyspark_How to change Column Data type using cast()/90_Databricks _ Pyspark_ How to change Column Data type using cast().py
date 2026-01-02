# Databricks notebook source
# MAGIC %md
# MAGIC #### **PySpark cast column**
# MAGIC
# MAGIC - In PySpark, you can **cast or change** the DataFrame column **data type** using **cast()** function of Column class.
# MAGIC - **Change Column Type** in PySpark DataframeUsing the **cast()** function
# MAGIC
# MAGIC    - using withColumn()
# MAGIC    - using selectExpr()
# MAGIC    - using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC - Below are the **subclasses** of the **DataType** classes in PySpark and we can **change or cast DataFrame columns** to **only these types**.
# MAGIC
# MAGIC     - NumericType
# MAGIC     - StringType
# MAGIC     - DateType
# MAGIC     - TimestampType
# MAGIC     - ArrayType
# MAGIC     - StructType
# MAGIC     - ObjectType
# MAGIC     - MapType
# MAGIC     - BinaryType
# MAGIC     - BooleanType
# MAGIC     - CalendarIntervalType
# MAGIC     - HiveStringType
# MAGIC     - NullType
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC      # Convert String to Integer Type
# MAGIC      df.withColumn("age", df.age.cast(IntegerType()))
# MAGIC      df.withColumn("age", df.age.cast('int'))
# MAGIC      df.withColumn("age", df.age.cast('integer'))
# MAGIC
# MAGIC      # withColumn
# MAGIC      df = df.withColumn("age", col("age").cast(StringType())) \
# MAGIC             .withColumn("isGraduated", col("isGraduated").cast(BooleanType())) \
# MAGIC             .withColumn("jobStartDate", col("jobStartDate").cast(DateType()))
# MAGIC      df.printSchema()
# MAGIC
# MAGIC      # Convert String to Date
# MAGIC      df.withColumn("Start_Date", to_date(col("Start_Date"), "dd-MMM-yyyy"))
# MAGIC
# MAGIC      # Convert Date to Long
# MAGIC      df.withColumn('Payment_Date', f.col('Payment_Date').cast(LongType()))
# MAGIC
# MAGIC      # Convert String to Boolean
# MAGIC      df.withColumn("isGraduated", col("isGraduated").cast(BooleanType()))

# COMMAND ----------

# MAGIC %md
# MAGIC      # select
# MAGIC      df.select(col("age").cast('int').alias("age"))

# COMMAND ----------

# MAGIC %md
# MAGIC      # selectExpr()
# MAGIC      df = df.selectExpr("cast(age as int) age",
# MAGIC                         "cast(isGraduated as string) isGraduated",
# MAGIC                         "cast(jobStartDate as string) jobStartDate")
# MAGIC      df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC      # SQL expression
# MAGIC      df.createOrReplaceTempView("CastExample")
# MAGIC      df = spark.sql("SELECT STRING(age),BOOLEAN(isGraduated),DATE(jobStartDate) from CastExample")
# MAGIC      df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 01**

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import lit, col, to_date, current_timestamp
from pyspark.sql.types import IntegerType, StringType, DoubleType, LongType, BooleanType, DateType

# COMMAND ----------

data =[{'rollno':'01', 'name':'sravan', 'age':23, 'height':5.79, 'weight':67, 'address':'Guntur'},
       {'rollno':'02', 'name':'ojaswi', 'age':26, 'height':3.79, 'weight':34, 'address':'Hyderabad'},
       {'rollno':'03', 'name':'gnanesh', 'age':37, 'height':2.79, 'weight':37, 'address':'Chennai'},
       {'rollno':'04', 'name':'rohith', 'age':29, 'height':3.69, 'weight':28, 'address':'Bangalore'},
       {'rollno':'05', 'name':'sridevi', 'age':45, 'height':5.59, 'weight':54, 'address':'Hyderabad'},
       {'rollno':'04', 'name':'Kiran', 'age':49, 'height':4.69, 'weight':38, 'address':'Bangalore'},
       {'rollno':'05', 'name':'Dhiraj', 'age':42, 'height':6.00, 'weight':34, 'address':'Nasik'},
       {'rollno':'05', 'name':'Dhiraj', 'age':42, 'height':6.00, 'weight':34, 'address':'Kolkata'},
       {'rollno':'05', 'name':'Dhiraj', 'age':42, 'height':6.00, 'weight':34, 'address':'Gurgaon'}]

# create the dataframe
df = spark.createDataFrame(data)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

df = df.select("*", lit(2).alias('source_system_id'),                       # integer
                    lit("2").alias('source_system_id'),                     # string
                    lit(2).cast(LongType()).alias('source_system_id')       # long
              )

display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 02**

# COMMAND ----------

column_names = ["language", "framework", "users", "backend", "date"]
data = [
    ("Python", "Django", "20000", "true", "2022-03-15"),
    ("Python", "FastAPI", "9000", "true", "2022-06-21"),
    ("Java", "Spring", "7000", "true", "2023-12-04"),
    ("JavaScript", "ReactJS", "5000", "false", "2023-01-11")
]
df4 = spark.createDataFrame(data, column_names)
display(df4)
df4.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Change Data Type of a Single Column**

# COMMAND ----------

# change column type
df_new = df4.withColumn("users", col("users").cast(IntegerType()))
display(df_new)

# print schema
df_new.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Change Data Type of Multiple Columns**
# MAGIC - Convert the data type of the column "users" from string to integer.
# MAGIC - Convert the data type of the column "backend" from string to boolean.
# MAGIC - Convert the data type of the column "date" from string to date.

# COMMAND ----------

# change column types
df_new1 = df4.withColumn("users", col("users").cast(IntegerType())) \
             .withColumn("backend", col("backend").cast(BooleanType())) \
             .withColumn("date", col("date").cast(DateType()))

# print schema
df_new1.printSchema()
