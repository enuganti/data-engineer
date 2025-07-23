# Databricks notebook source
# MAGIC %md
# MAGIC **Why lit() is Used**
# MAGIC
# MAGIC       array([lit(x) for x in fixed_values_cust])
# MAGIC       
# MAGIC - The elements of **fixed_values_cust** are simple **Python integers**, and to use them in Spark expressions like **array**, they must be **converted** to **PySpark column-compatible** literals.
# MAGIC
# MAGIC - **Without lit()**, Spark would **not recognize** the elements as **valid column expressions**, and the code would throw an **error**.

# COMMAND ----------

# Define the fixed integer values
fixed_values_cust = [10, 13, 15, 20, 25, 28, 30, 35, 38, 40, 45]

[x for x in fixed_values_cust]

# COMMAND ----------

from pyspark.sql.functions import lit

[lit(x) for x in fixed_values_cust]

# COMMAND ----------

# MAGIC %md
# MAGIC      [
# MAGIC         Column<'10'>,  # A PySpark column object for the literal value 10
# MAGIC         Column<'13'>,  # A PySpark column object for the literal value 13
# MAGIC         Column<'15'>,  # A PySpark column object for the literal value 15
# MAGIC         Column<'20'>,  # A PySpark column object for the literal value 20
# MAGIC         Column<'25'>,  # A PySpark column object for the literal value 25
# MAGIC         Column<'28'>,  # A PySpark column object for the literal value 28
# MAGIC         Column<'30'>, # A PySpark column object for the literal value 30
# MAGIC         Column<'35'>, # A PySpark column object for the literal value 35
# MAGIC         Column<'38'>, # A PySpark column object for the literal value 38
# MAGIC         Column<'40'>, # A PySpark column object for the literal value 40
# MAGIC         Column<'45'>  # A PySpark column object for the literal value 45
# MAGIC      ]

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Explanation:**
# MAGIC
# MAGIC       array([lit(x) for x in fixed_values_cust]
# MAGIC
# MAGIC **fixed_values_cust:**
# MAGIC - This is a **Python list** containing predefined **numeric values**:
# MAGIC
# MAGIC     [10, 13, 15, 20, 25, 28, 30, 35, 38, 40, 45]
# MAGIC
# MAGIC **lit(x):**
# MAGIC
# MAGIC - The **lit()** function in PySpark creates a **column object** representing a **literal value (a constant)**.
# MAGIC
# MAGIC - For each element **x** in the **fixed_values_cust** list, **lit(x)** converts it into a **PySpark literal column**.
# MAGIC
# MAGIC - **lit(x)** converts each element **x** from the **Python list** a into a **Spark Column type**, which is necessary for the **array()** function to create a **new array column** within the **DataFrame**.
# MAGIC
# MAGIC **List Comprehension:**
# MAGIC
# MAGIC - The comprehension **[lit(x) for x in fixed_values_cust]** iterates over every value **x** in the list **fixed_values_cust** and applies the **lit(x)** function to it.
# MAGIC
# MAGIC - As a result, it produces a new list where each item is a PySpark column object representing the corresponding value from **fixed_values_cust**.

# COMMAND ----------

# MAGIC %md
# MAGIC - Imagine you want to add a new column named **source** with the value **manual** to every row of your DataFrame.
# MAGIC
# MAGIC - You would use **df.withColumn("source", lit("manual"))**.
# MAGIC
# MAGIC - If you tried **df.withColumn("source", "manual")** directly, it would likely result in an **error** because **manual is a Python string, not a PySpark Column**.
# MAGIC
# MAGIC - While **list comprehensions** themselves are a **Python construct** for creating **lists**, their **integration** with **PySpark** operations often necessitates **lit()** to **bridge the gap** between **Python literals and PySpark's Column-based operations**.

# COMMAND ----------

from pyspark.sql.functions import when, col, array, array_contains

# COMMAND ----------

# Converts the `fixed_values_cust` Python list into a `PySpark array column` where each element is wrapped as a literal (`lit`).
array([lit(x) for x in fixed_values_cust])

# COMMAND ----------

array(*[lit(x) for x in fixed_values_cust])

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

df = spark.range(1)  # dummy row
fixed_values_cust = [10, 13, 15]

# Case 1 - without unpacking
df1 = df.withColumn("array_col_wrong", array([lit(x) for x in fixed_values_cust]))

# Case 2 - with unpacking
df2 = df.withColumn("array_col_right", array(*[lit(x) for x in fixed_values_cust]))

display(df1)
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC      +-------------------+
# MAGIC      |  array_col_wrong  |
# MAGIC      +-------------------+
# MAGIC      |  [[10, 13, 15]]   |    <-- nested array (1 element inside)
# MAGIC      +-------------------+
# MAGIC
# MAGIC      +-------------------+
# MAGIC      |  array_col_right  |
# MAGIC      +-------------------+
# MAGIC      |   [10, 13, 15]    |    <-- correct array
# MAGIC      +-------------------+
# MAGIC

# COMMAND ----------

df1.explain(True)
df2.explain(True)

# COMMAND ----------

data = [(1, "Rakesh", 25, "Sales"),
        (2, "Kiran", 29, "Admin"),
        (3, "Preeti", 31, "Marketing"),
        (4, "Subash", 33, "HR"),
        (5, "Sekhar", 35, "Maintenance"),
        (6, "Nirmal", 55, "Security"),
        (7, "Sailesh", 35, "IT"),
        (8, "kumar", 29, "Sales"),
        (9, "Asif", 39, "HR"),
        (10, "Murugan", 40, "Admin"),
        (11, "Prakash", 45, "Marketing")]

columns = ["id", "Name", "Age", "Department"]

df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

# DBTITLE 1,Error: without lit()
# Add a column with random fixed values
df_with_fixed_value33 = df.withColumn('fixed_value', array([x for x in fixed_values_cust]))
display(df_with_fixed_value33)

# COMMAND ----------

# Add a column with random fixed values
df_with_fixed_value3 = df.withColumn('fixed_value', array([lit(x) for x in fixed_values_cust])) \
                         .withColumn('fixed_value_unpack', array(*[lit(x) for x in fixed_values_cust]))
display(df_with_fixed_value3)

# COMMAND ----------

# Add a column with random fixed values
df_with_fixed_value4 = df.withColumn(
    'fixed_value', 
    array([lit(x) for x in fixed_values_cust])[0]
)
display(df_with_fixed_value4)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filtering Rows (to check if a column value is in a fixed list?)

# COMMAND ----------

fixed_values_cust = [10, 13, 15, 20, 25, 28, 30, 35, 38, 40, 45]

# Create an array literal and check if Age is in that array
df_filtered = df.withColumn('fixed_value', array([lit(x) for x in fixed_values_cust])) \
                .filter(array_contains(array(*[lit(x) for x in fixed_values_cust]), col("Age")))

display(df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC - **array()** needs **multiple column expressions**, not a **single list** of them.
# MAGIC - `*` operator (called **unpacking** in Python).
# MAGIC - `*` unpacks the list into individual arguments.
# MAGIC
# MAGIC **without `*`:**
# MAGIC
# MAGIC         array([lit(10), lit(13), lit(15), lit(20), lit(25), lit(28), lit(30), lit(35), lit(38), lit(40), lit(45)])
# MAGIC
# MAGIC - This passes a **single list** as **one argument** to array(), which is **not valid for PySpark's array()** function. It expects **multiple column arguments**, not a list of columns.
# MAGIC
# MAGIC **with `*`:**
# MAGIC
# MAGIC       array(*[lit(10), lit(13), lit(15), lit(20), lit(25), lit(28), lit(30), lit(35), lit(38), lit(40), lit(45)])
# MAGIC
# MAGIC       # This unpacks the list so that it becomes:
# MAGIC       array(lit(10), lit(13), lit(15), lit(20), lit(25), lit(28), lit(30), lit(35), lit(38), lit(40), lit(45))
# MAGIC
# MAGIC
# MAGIC - **array(*[lit(x) for x in fixed_values_cust])** creates a **literal** array column from your list.
# MAGIC - **array_contains(..., col("Age"))** checks if the **Age value exists** in that **array**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using in when/case Expressions

# COMMAND ----------

fixed_values_cust = [10, 13, 15, 20, 25, 28, 30, 35, 38, 40, 45]

df_caseWhen = df.withColumn(
    "is_fixed",
    when(col("Age").isin(*fixed_values_cust), lit(True)).otherwise(lit(False))
)
display(df_caseWhen)