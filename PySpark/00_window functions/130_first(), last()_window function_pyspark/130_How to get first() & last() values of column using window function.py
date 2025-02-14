# Databricks notebook source
# MAGIC %md
# MAGIC **first:**
# MAGIC - is one of **window function** which returns **first value** of a column of each window.
# MAGIC
# MAGIC       df.withColumn("FirstValue", first("columnA").over(Window.partitionBy("ColumnB").orderBy("ColumnC")))
# MAGIC
# MAGIC **last:**
# MAGIC - is one of **window function** which returns **last value** of a column of each window.
# MAGIC
# MAGIC       df.withColumn("LastValue", last("columnA").over(Window.partitionBy("ColumnB").orderBy("ColumnC")))

# COMMAND ----------

# MAGIC %md
# MAGIC      ignorenulls: Column or str
# MAGIC       - if first value is null then look for first non-null value.

# COMMAND ----------

from pyspark.sql.functions import first, last, to_date, col

# COMMAND ----------

# MAGIC %md
# MAGIC #### **PySpark**

# COMMAND ----------

data = [("Prakash", "IT", 8000, "2023-03-15"),
        ("Syamala", "Finance", 7600, "2023-04-16"),
        ("Ritesh", "IT", 5100, "2023-05-10"),	
        ("Robert", "Marketing", 4000, "2023-06-25"),
        ("Harsha", "Sales", 2000, "2023-07-27"),
        ("Harsha", "Sales", None, "2023-08-11"),
        ("Senthil", "Finance", 3500, "2023-09-12"),
        ("Parthiv", "IT", 4900, "2023-10-13"),
        ("Prabhav", "Marketing", 4000, "2023-11-19"),
        ("Prabhav", "Marketing", None, "2023-12-20"),
        ("Pandya", "IT", 3000, "2023-01-01"),
        ("Anil", "Sales", 5100, "2024-10-04"),
        ("Anil", "Sales", None, "2024-09-08")
        ]
schema = ["employee_name", "department", "salary", "start_date"]

df = spark.createDataFrame(data, schema)

# convert the "date" data type
df = df.withColumn("start_date", to_date(col("start_date")))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Using first() and last() with orderBy()**

# COMMAND ----------

df = df.orderBy(col("salary"))
display(df)

# COMMAND ----------

# First and Last based on ordering by Salary
df.select(first("salary").alias("First_Salary"), last("salary").alias("Last_Salary")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Using first() and last() with select()**

# COMMAND ----------

# Returns the first row as a Row
df.first()

# COMMAND ----------

# DBTITLE 1,first
# Using first() function
df.select(first("salary")).display()	

# COMMAND ----------

# DBTITLE 1,first non-null value
# To return the first non-null value instead:
df.select(first(df.salary, ignorenulls=True)).display()

# COMMAND ----------

# DBTITLE 1,last
# Using last() function
df.select(last("salary")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Using first() and last() with groupBy()**

# COMMAND ----------

df.orderBy(col("department"), col("salary")).display()

# COMMAND ----------

# DBTITLE 1,groupBy with first
df.groupBy("department") \
  .agg(first("salary").alias("First_Salary"), last("salary").alias("Last_Salary")) \
  .display()

# COMMAND ----------

# DBTITLE 1,groupBy with first & ignorenulls
df.groupBy("department") \
  .agg(first("salary", ignorenulls=True).alias("First_Salary"), last("salary", ignorenulls=True).alias("Last_Salary")) \
  .orderBy("department") \
  .display()

# COMMAND ----------

# DBTITLE 1,window function: "salary"
from pyspark.sql.window import Window
df_window_null = df.withColumn("first_salary", first("salary").over(Window.partitionBy("department"))) \
                   .withColumn("last_salary", last("salary").over(Window.partitionBy("department"))) \
                   .orderBy("department")
display(df_window_null)

# COMMAND ----------

# DBTITLE 1,window function: "salary", ignorenulls
df_window_ignore = df.withColumn("first_salary", first("salary", ignorenulls=True).over(Window.partitionBy("department"))) \
                     .withColumn("last_salary", last("salary", ignorenulls=True).over(Window.partitionBy("department"))) \
                     .orderBy("department")
display(df_window_ignore)

# COMMAND ----------

# DBTITLE 1,window function: "start_date"
df_window = df.withColumn("first_start_date", first("start_date").over(Window.partitionBy("department"))) \
              .withColumn("last_start_date", last("start_date").over(Window.partitionBy("department"))) \
              .orderBy("department")
display(df_window)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Spark SQL**

# COMMAND ----------

# Convert DataFrame to temparory view
df.createOrReplaceTempView("transaction")

# COMMAND ----------

spark_sql = spark.sql("""SELECT FIRST(salary) AS First_Salary, LAST(salary) AS Last_Salary,
                                FIRST(start_date) AS First_start_date, LAST(start_date) AS Last_start_date
                         FROM transaction
                      """)

display(spark_sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT department,
# MAGIC        FIRST(salary) OVER (PARTITION BY department) AS first_salary,
# MAGIC        LAST(salary) OVER (PARTITION BY department) AS last_salary,
# MAGIC        FIRST(start_date) OVER (PARTITION BY department) AS first_start_date,
# MAGIC        LAST(start_date) OVER (PARTITION BY department) AS last_start_date
# MAGIC FROM transaction
# MAGIC ORDER BY department;
