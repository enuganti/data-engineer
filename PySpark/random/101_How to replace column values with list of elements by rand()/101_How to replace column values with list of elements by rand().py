# Databricks notebook source
from pyspark.sql.functions import rand, lit, array

# COMMAND ----------

# Create a sample DataFrame
data = [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)]
df = spark.createDataFrame(data, ["id"])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Using rand() to Randomly Select Elements from a List**

# COMMAND ----------

# MAGIC %md
# MAGIC      # Define the fixed integer values
# MAGIC      fixed_values_cust = [25, 30, 40, 55, 70, 85, 100, 130, 150, 145, 160]
# MAGIC
# MAGIC      # Add a column with random fixed values
# MAGIC      df_with_fixed_value = df.withColumn(
# MAGIC                       'fixed_value', 
# MAGIC                       array([lit(x) for x in fixed_values_cust])[(rand() * len(fixed_values_cust)).cast('int')]
# MAGIC      )
# MAGIC      display(df_with_fixed_value)

# COMMAND ----------

# Define the fixed integer values
# A list of predefined fixed values that you want to use as random selections for the fixed_value column.
fixed_values_cust = [25, 30, 40, 55, 70, 85, 100, 130, 150, 145, 160]

len(fixed_values_cust)

# COMMAND ----------

# DBTITLE 1,Generate Random integer values b/n 0 to 11
# Add a column with random fixed values
# Used to pick a random index from the array
df_with_fixed_value2 = df.withColumn(
    'fixed_value', (rand() * len(fixed_values_cust)).cast('int')
)
display(df_with_fixed_value2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Why lit() is Used**
# MAGIC - PySpark operations work on **columns and expressions**.
# MAGIC - The elements of **fixed_values_cust** are simple **Python integers**, and to use them in Spark expressions like **array**, they must be **converted** to **PySpark column-compatible** literals.
# MAGIC - Without lit(), Spark would **not recognize** the elements as **valid column expressions**, and the code would throw an **error**.

# COMMAND ----------

[x for x in fixed_values_cust]

# COMMAND ----------

# MAGIC %md
# MAGIC **Explanation:**
# MAGIC - **fixed_values_cust:** This is a Python list containing predefined numeric values:
# MAGIC
# MAGIC     [25, 30, 40, 55, 70, 85, 100, 130, 150, 145, 160].
# MAGIC
# MAGIC **lit(x):**
# MAGIC
# MAGIC - The `lit()` function in PySpark creates a column object representing a `literal value (a constant)`.
# MAGIC - For each element `x` in the `fixed_values_cust` list, `lit(x)` converts it into a `PySpark literal column`.
# MAGIC
# MAGIC **List Comprehension:**
# MAGIC
# MAGIC - The comprehension `[lit(x) for x in fixed_values_cust]` iterates over every value `x` in the list `fixed_values_cust` and applies the `lit(x)` function to it.
# MAGIC - As a result, it produces a new list where each item is a PySpark column object representing the corresponding value from `fixed_values_cust`.

# COMMAND ----------

[lit(x) for x in fixed_values_cust]

# COMMAND ----------

# MAGIC %md
# MAGIC      [
# MAGIC         Column<'25'>,  # A PySpark column object for the literal value 25
# MAGIC         Column<'30'>,  # A PySpark column object for the literal value 30
# MAGIC         Column<'40'>,  # A PySpark column object for the literal value 40
# MAGIC         Column<'55'>,  # A PySpark column object for the literal value 55
# MAGIC         Column<'70'>,  # A PySpark column object for the literal value 70
# MAGIC         Column<'85'>,  # A PySpark column object for the literal value 85
# MAGIC         Column<'100'>, # A PySpark column object for the literal value 100
# MAGIC         Column<'130'>, # A PySpark column object for the literal value 130
# MAGIC         Column<'150'>, # A PySpark column object for the literal value 150
# MAGIC         Column<'145'>, # A PySpark column object for the literal value 145
# MAGIC         Column<'160'>  # A PySpark column object for the literal value 160
# MAGIC      ]

# COMMAND ----------

# Converts the `fixed_values_cust` Python list into a `PySpark array column` where each element is wrapped as a literal (`lit`).
array([lit(x) for x in fixed_values_cust])

# COMMAND ----------

# MAGIC %md
# MAGIC **Why Use array for List Comprehension?**
# MAGIC
# MAGIC - **Consolidate Fixed Values into a Single Data Structure**:
# MAGIC
# MAGIC   - The **[lit(x) for x in fixed_values_cust]** generates a **list of PySpark literal column objects**. However, PySpark operations, such as **indexing or random selection**, cannot directly operate on a Python list.
# MAGIC   
# MAGIC   - The **array()** function **combines** these individual **column literals into a single PySpark array column**, which is a valid column type for further DataFrame operations.

# COMMAND ----------

# Add a column with random fixed values
df_with_fixed_value3 = df.withColumn(
    'fixed_value', 
    array([lit(x) for x in fixed_values_cust])
)
display(df_with_fixed_value3)

# COMMAND ----------

# Add a column with random fixed values
df_with_fixed_value4 = df.withColumn(
    'fixed_value', 
    array([lit(x) for x in fixed_values_cust])[0]
)
display(df_with_fixed_value4)

# COMMAND ----------

# Add a column with random fixed values
df_with_fixed_value = df.withColumn(
    'fixed_value', 
    array([lit(x) for x in fixed_values_cust])[len(fixed_values_cust)-1]
)
display(df_with_fixed_value)

# COMMAND ----------

# Define the fixed integer values
fixed_values_cust = [25, 30, 40, 55, 70, 85, 100, 130, 150, 145, 160]

# Add a column with random fixed values
df_with_fixed_value = df.withColumn(
    'fixed_value', 
    array([lit(x) for x in fixed_values_cust])[(rand() * len(fixed_values_cust)).cast('int')]
)
display(df_with_fixed_value)

# COMMAND ----------

# MAGIC %md
# MAGIC The expression **[(rand() * len(fixed_values_cust)).cast('int')]** is used to generate a **random index** within the range of the **fixed_values_cust** list. 
# MAGIC
# MAGIC **rand():**
# MAGIC - Generates a random float value **between 0 and 1**.
# MAGIC
# MAGIC **rand() * len(fixed_values_cust):**
# MAGIC - **Multiplies** the **random float** by the **length of the fixed_values_cust** list, which scales the random value to the range **[0, len(fixed_values_cust))**.
# MAGIC
# MAGIC **(rand() * len(fixed_values_cust)).cast('int'):**
# MAGIC - Casts the scaled random **float to an integer**, effectively generating a random **index** within the range of the list indices.
# MAGIC
# MAGIC **[(rand() * len(fixed_values_cust)).cast('int')]:**
# MAGIC - The result is wrapped in a **list** to be used as an **index** for **array selection**.
