# Databricks notebook source
import random
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 01**

# COMMAND ----------

# DBTITLE 1,sample dataframe 1
# Sample DataFrame
data = [(1,25), (2,35), (3,33), (4,39), (5,29), (6,22), (7,45), (8,48), (9,37), (10,55)]
df = spark.createDataFrame(data, ["id", "age"])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **a) Add a Random Integer Column**

# COMMAND ----------

# Define a UDF for generating random integers
random_udf = udf(lambda: random.randint(1, 100), IntegerType())

# Add a new column with random integers
df_with_random = df.withColumn('random_int', random_udf())

display(df_with_random)

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Random Integers Within a Specific Range**

# COMMAND ----------

# UDF to generate random integers in a given range (e.g., 50 to 100)
random_in_range_udf = udf(lambda: random.randint(50, 100), IntegerType())

# Add a new column with random integers in the range 50 to 100
df_with_random_range = df.withColumn('random_in_range', random_in_range_udf())

display(df_with_random_range)

# COMMAND ----------

# MAGIC %md
# MAGIC **c) Replace Existing Column Values with Random Integers**

# COMMAND ----------

# Replace the `id` column with random integers between 10 and 20
df_with_replaced_values = df.withColumn('age', random_in_range_udf())

display(df_with_replaced_values)

# COMMAND ----------

# MAGIC %md
# MAGIC **d) Random Integers Based on an Existing Column Value**

# COMMAND ----------

# UDF to generate random integers based on the existing column value
random_based_on_column_udf = udf(lambda x: random.randint(x, x + 10), IntegerType())

# Add a new column with random integers based on the `id` column
df_with_random_based = df.withColumn('random_based_on_id', random_based_on_column_udf(df['id']))

display(df_with_random_based)

# COMMAND ----------

# MAGIC %md
# MAGIC **e) Random Integers for Grouped Data**

# COMMAND ----------

# Group data and assign random values per group
df_grouped = df.groupBy().agg(
    f.lit(random.randint(1, 100)).alias('random_group_value')
)

display(df_grouped)

# COMMAND ----------

# MAGIC %md
# MAGIC **f) Using randint for Conditional Column Values**

# COMMAND ----------

# UDF for conditional random values
conditional_random_udf = udf(lambda x: random.randint(1, 50) if x % 2 == 0 else random.randint(51, 100), IntegerType())

# Add a column with random values based on a condition
df_with_conditional_random = df.withColumn('conditional_random', conditional_random_udf(df['id']))

display(df_with_conditional_random)

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 02**

# COMMAND ----------

# DBTITLE 1,sample dataframe 2
# Sample DataFrame with 10 rows
data = [(1, "Alice", "HR", 5000, 1),
        (2, "Bob", "IT", 6000, 2),
        (3, "Cathy", "HR", 5500, 3),
        (4, "David", "IT", 7000, 4),
        (5, "Eve", "HR", 8000, 5),
        (6, "Frank", "IT", 7500, 6),
        (7, "Grace", "HR", 6200, 7),
        (8, "Hank", "IT", 6800, 8),
        (9, "Ivy", "HR", 7800, 9),
        (10, "Jack", "IT", 6700, 10)]

columns = ["ID", "Name", "Department", "Salary", "cust_value"]

# Create DataFrame
df1 = spark.createDataFrame(data, columns)
display(df1)

# COMMAND ----------

# DBTITLE 1,@udf
# Define UDF for random salary values (e.g., between 4000 and 10000)
@udf(IntegerType())
def random_salary():
    return random.randint(4000, 10000)

# Define UDF for random cust_value values (e.g., between 1 and 20)
@udf(IntegerType())
def random_cust_value():
    return random.randint(1, 20)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Replace random integers for single column**

# COMMAND ----------

# Replace the `cust_value` column with random integers
df_with_random_values = df1.withColumn("cust_value", random_in_range_udf())

# Show the result
display(df_with_random_values)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Replace random integers for multiple columns**

# COMMAND ----------

# Update the columns with random values
df_updated = df1.withColumn("Salary", random_salary())\
                .withColumn("cust_value", random_cust_value())

# Show the result
display(df_updated)
