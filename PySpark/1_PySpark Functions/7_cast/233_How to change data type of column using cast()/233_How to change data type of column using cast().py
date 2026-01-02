# Databricks notebook source
# MAGIC %md
# MAGIC ##### PySpark cast column
# MAGIC - In PySpark, you can **change data type** of column using **cast()** function.

# COMMAND ----------

# MAGIC %md
# MAGIC Below are the **subclasses** of the **DataType** classes in PySpark and we can change or cast DataFrame columns to only these types.
# MAGIC
# MAGIC - `NumericType`
# MAGIC - `StringType`
# MAGIC - `DateType`
# MAGIC - `TimestampType`
# MAGIC - ArrayType
# MAGIC - StructType
# MAGIC - ObjectType
# MAGIC - MapType
# MAGIC - BinaryType
# MAGIC - `BooleanType`
# MAGIC - CalendarIntervalType
# MAGIC - HiveStringType
# MAGIC - NullType

# COMMAND ----------

# MAGIC %md
# MAGIC **Common Functionality**
# MAGIC
# MAGIC ✅ Converts **string or timestamp** into a **date**, but assumes **yyyy-MM-dd** format.
# MAGIC
# MAGIC ✅ If the format **does not match**, it returns **NULL**.
# MAGIC
# MAGIC ✅ Cannot specify a **custom format** like **to_date()**.
# MAGIC
# MAGIC ✅ **Removes** the **time** portion from **TimestampType**, keeping **only the date**.
# MAGIC
# MAGIC ##### .cast("date")
# MAGIC
# MAGIC - In Spark, **.cast("date")** is a **shorthand** way to **cast** a column to **DateType**.
# MAGIC
# MAGIC - **No** need to **import DateType**.
# MAGIC
# MAGIC - Internally, **Spark converts "date" to DateType()**.
# MAGIC
# MAGIC ##### cast(DateType())
# MAGIC
# MAGIC ✅ Equivalent to **.cast("date")**, but more explicit in code.
# MAGIC
# MAGIC | col_name	             | After col_name.cast(DateType()) | After col_name.cast("date")  |
# MAGIC |------------------------|---------------------------------|------------------------------|
# MAGIC | "2024-03-06"	         | 2024-03-06 (Date)               |    2024-03-06 (Date)         |
# MAGIC | "06-03-2024"	         | NULL (Format mismatch)          |    NULL (Format mismatch)    |
# MAGIC | "2024-03-06 12:30:00"	 | 2024-03-06 (Time removed)       |    2024-03-06 (Time removed) |

# COMMAND ----------

# MAGIC %md
# MAGIC **✔ When cast() works?**
# MAGIC
# MAGIC - **String format** must be:
# MAGIC
# MAGIC       yyyy-MM-dd HH:mm:ss -> default timestamp format
# MAGIC       yyyy-MM-dd          -> default date format
# MAGIC
# MAGIC **❌ When cast() returns NULL?**
# MAGIC
# MAGIC       "15-07-2024 10:30:00"   ❌  -> custom timestamp format
# MAGIC       "2024/07/15 10:30"     ❌   -> custom timestamp format

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType

# COMMAND ----------

data = [(101, 'Hitesh', 'baleno', '2507623', 25.1, "3000.6089", 'FALSE', "2006-01-01", "2024-07-15 10:30:00"),
        (102, 'Kiran', 'alto', '2012345', 28.6, "3300.8067", 'TRUE', "1980-01-10", "2025-04-01 08:15:00"),
        (103, 'Adarsh', 'swift', '3045893', 32.4, "5000.5034", 'FALSE', "1985-11-19", "2024-06-11 18:25:55"),
        (104, 'Kamal', 'city', '3512678', 43.8, "4550.5034", 'TRUE', "2025-05-28", "2023-08-17 22:55:35"),
        (105, 'Prakash', 'dzire', '2267934', 62.5, "6780.5034", 'FALSE', "2025-09-16", "2025-04-01 20:45:22")]

columns = ['SNo', 'name', 'product', 'amount', 'avg', 'Salary', 'is_discounted', "jobStartDate", "timestamp_str"]

df_initial = spark.createDataFrame(data, columns)
display(df_initial)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 1
# MAGIC - Using **col().cast()**

# COMMAND ----------

df_initial.withColumn("Salary", df_initial.Salary.cast('double')).printSchema()    
# # df_initial.withColumn("Salary", df_initial.Salary.cast(DoubleType())).printSchema()
# df_initial.withColumn("Salary", col("Salary").cast('double')).printSchema()

# COMMAND ----------

# DBTITLE 1,method 01
df_cast_type01 = df_initial\
  .select(
    col('SNo').cast('integer'),
    col('name'),
    col('product'),
    col('amount').cast('long'),
    col('avg'),
    col('Salary').cast('double'),
    col('is_discounted').cast('boolean'),
    col("jobStartDate").cast('date'),
    col("timestamp_str").cast('timestamp'),
    col("timestamp_str").cast('date').alias('ts_date_format')
  )
display(df_cast_type01)

# COMMAND ----------

# DBTITLE 1,method 02
from pyspark.sql.types import IntegerType, StringType, DoubleType, BooleanType, DateType, TimestampType, LongType

df_cast_type02 = df_initial\
  .select(
    col('SNo').cast(IntegerType()),
    col('name'),
    col('product'),
    col('amount').cast(LongType()),
    col('avg'),
    col('Salary').cast(DoubleType()),
    col('is_discounted').cast(BooleanType()),
    col("jobStartDate").cast(DateType()),
    col("timestamp_str").cast(TimestampType()),
    col("timestamp_str").cast(DateType()).alias('ts_date_format')
  )
display(df_cast_type02)

# COMMAND ----------

# MAGIC %md
# MAGIC - I prefer the **first option** of these **two** since I **don’t** have to **import the types** but either should work.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 2
# MAGIC - Using **.withColumn()**

# COMMAND ----------

df_cast_with_type01 = df_initial\
  .withColumn('SNo', col('SNo').cast(IntegerType()))\
  .withColumn('amount', col('amount').cast(LongType()))\
  .withColumn('Salary', col('Salary').cast(DoubleType())) \
  .withColumn('is_discounted', col('is_discounted').cast(BooleanType())) \
  .withColumn('jobStartDate', col('jobStartDate').cast(DateType())) \
  .withColumn('timestamp_str', col('timestamp_str').cast(TimestampType())) \
  .withColumn('ts_date_format', col("timestamp_str").cast(DateType()))

display(df_cast_with_type01)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 3
# MAGIC - Using a Python **dictionary and .withColumn()**

# COMMAND ----------

data_types = {
    'SNo': 'integer',
    'amount': 'long',
    'Salary': 'double',
    'is_discounted': 'boolean',
    'jobStartDate': 'date',
    'timestamp_str': 'timestamp'
}

df_dict = df_initial
for column_name, data_type in data_types.items():
    df_dict = df_dict.withColumn(column_name, col(column_name).cast(data_type))

display(df_dict)

# COMMAND ----------

data_types = {
    'SNo': IntegerType(),
    'amount': LongType(),
    'Salary': DoubleType(),
    'is_discounted': BooleanType(),
    'jobStartDate': DateType(),
    'timestamp_str': TimestampType()
  }

for column_name, data_type in data_types.items():
    df_dict = df_dict.withColumn(column_name, col(column_name).cast(data_type))

display(df_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 4
# MAGIC - Using **col().cast()** with a Python **dictionary** and Python **list comprehension**

# COMMAND ----------

# This returns a list of tuples
df_initial.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC - This returns a **list of tuples**.
# MAGIC - **Each tuple** has:
# MAGIC       
# MAGIC       (column_name, current_data_type)

# COMMAND ----------

# [col for col in df_initial.dtypes]
[column_schema for column_schema in df_initial.dtypes]

# COMMAND ----------

# MAGIC %md
# MAGIC - Each **column_schema** looks like:
# MAGIC
# MAGIC       ('SNo', 'bigint')
# MAGIC
# MAGIC       column_schema[0] → column name
# MAGIC       column_schema[1] → data type

# COMMAND ----------

[col(column_schema[0]) for column_schema in df_initial.dtypes]

# COMMAND ----------

[col(column_schema[1]) for column_schema in df_initial.dtypes]

# COMMAND ----------

data_type_map = {
    'SNo': 'integer',
    'amount': 'long',
    'Salary': 'double',
    'is_discounted': 'boolean',
    'jobStartDate': 'date',
    'timestamp_str': 'timestamp'
  }

# COMMAND ----------

[col(column_schema[0]).cast(data_type_map.get(column_schema[0], column_schema[1])) for column_schema in df_initial.dtypes]

# COMMAND ----------

# MAGIC %md
# MAGIC      data_type_map.get(column_schema[0], column_schema[1])
# MAGIC
# MAGIC - If the column `exists` in **data_type_map**, `use that type`. Otherwise, keep the **original type**.
# MAGIC
# MAGIC |     Column        | Found in map? |    Before    |     After     |
# MAGIC | ----------------- | ------------- | -------------| --------------|
# MAGIC |   SNo             |      ✅       | long         |   integer     |
# MAGIC |   name            |      ❌       | string       |   string      |
# MAGIC |   product         |      ❌       | string       |   string      |
# MAGIC |   amount          |      ✅       | string       |   long        |
# MAGIC |   avg             |      ❌       | double       |   double      |
# MAGIC |   Salary          |      ✅       | string       |   double      |
# MAGIC |   is_discounted   |      ✅       | string       |  boolean      |
# MAGIC |   jobStartDate    |      ✅       | string       |  date         |
# MAGIC |   timestamp_str   |      ✅       | string       |  timestamp    |

# COMMAND ----------

# MAGIC %md
# MAGIC      col(column_schema[0]).cast(target_type)
# MAGIC
# MAGIC - `Selects a column`.
# MAGIC - `Changes its data type`.
# MAGIC
# MAGIC **What does the list comprehension produce?**
# MAGIC
# MAGIC       [
# MAGIC         col("SNo").cast("integer"),
# MAGIC         col("amount").cast("long"),
# MAGIC         col("Salary").cast("double"),
# MAGIC         col("is_discounted").cast("boolean"),
# MAGIC         col("jobStartDate").cast("date"),
# MAGIC         col("timestamp_str").cast("timestamp")
# MAGIC       ]

# COMMAND ----------

data_type_map = {
    'SNo': 'integer',
    'amount': 'long',
    'Salary': 'double',
    'is_discounted': 'boolean',
    'jobStartDate': 'date',
    'timestamp_str': 'timestamp'
  }

df_dict_list_01 = df_initial\
  .select([col(column_schema[0]).cast(data_type_map.get(column_schema[0], column_schema[1])) for column_schema in df_initial.dtypes])

display(df_dict_list_01)

# COMMAND ----------

data_type_map = {
    'SNo': IntegerType(),
    'amount': LongType(),
    'Salary': DoubleType(),
    'is_discounted': BooleanType(),
    'jobStartDate': DateType(),
    'timestamp_str': TimestampType()
  }

df_dict_list_02 = df_initial\
  .select([col(column_schema[0]).cast(data_type_map.get(column_schema[0], column_schema[1])) for column_schema in df_initial.dtypes])

display(df_dict_list_02)