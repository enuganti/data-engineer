# Databricks notebook source
# MAGIC %md
# MAGIC #### unionByName()
# MAGIC
# MAGIC - **union()** requires both DataFrames to have the **same schema** in the **same order**.
# MAGIC - **unionByName()** allows unioning by **matching column names** instead of relying on **order**.
# MAGIC - use With **allowMissingColumns=True**, it will handle **mismatched schemas** by **filling missing columns** with **null**.
# MAGIC - Use when **column order** differs **between DataFrames**.

# COMMAND ----------

# MAGIC %md
# MAGIC - The **unionByName()** function in PySpark is used to **combine two or more DataFrames** based on their **column names**, rather than their **positional order**.
# MAGIC - This is a key **distinction** from the standard **union() or unionAll()** methods, which require the DataFrames to have **identical schemas** in terms of **both column names and order**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Content
# MAGIC - Why need to use **unioByName** instead of **union**?
# MAGIC - **unionByName**
# MAGIC   - same schema
# MAGIC   - different column order
# MAGIC   - different schemas
# MAGIC   - Union Multiple DataFrames
# MAGIC   - Union of two DataFrames with few common columns
# MAGIC   - Union of two DataFrames with completely different columns

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Why need to use unioByName instead of union?**

# COMMAND ----------

# First DataFrame
data = [("Andrew", "New York", 25),
        ("Bibin", "Chicago", 30),
        ("Alex", "Delhi", 22),
        ("Chetan", "Nasik", 23),
        ("Albert", "Kolkatta", 27)
        ]

columns = ["Name", "Country", "Age"]

df1_union = spark.createDataFrame(data, columns)
df1_union.display()

# COMMAND ----------

# First DataFrame
data = [("Abhi", "INDIA", 20),
        ("Johny", "UK", 22),
        ("Charan", "SWEDEN", 25),
        ("Harish", "NORWAY", 28),
        ("Kiran", "GERMANY", 29)
        ]

columns = ["Name", "Country", "Age"]

df2_union = spark.createDataFrame(data, columns)
df2_union.display()

# COMMAND ----------

# Order of columns are same
# df1 & df2: Name, Country, Age
df1_union.union(df2_union).display()

# COMMAND ----------

# First DataFrame
data = [("Andrew", 31, "INDIA"),
        ("Bibin", 32, "CANADA"),
        ("Alex", 33, "USA"),
        ("Chetan", 34, "Nepal"),
        ("Albert", 35, "UK")
        ]

columns = ["Name", "Age", "Country"]

df3_union = spark.createDataFrame(data, columns)
df3_union.display()

# COMMAND ----------

# Order of columns are not same
# df1: Name, Country, Age
# df3: Name, Age, Country

df1_union.union(df3_union).display()

# COMMAND ----------

# Order of columns are not same
# df1: Name, Country, Age
# df3: Name, Age, Country

df1_union.unionByName(df3_union).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Same schema**

# COMMAND ----------

# First DataFrame
data = [(1, "Andrew", 25, "F", "New York"),
        (2, "Bibin", 30, "M", "Chicago"),
        (3, "Alex", 22, "F", "Delhi"),
        (4, "Chetan", 23, "M", "Nasik"),
        (5, "Albert", 27, "F", "Kolkatta"),
        (6, "Mansoor", 32, "M", "Pune"),
        (7, "Diliph", 29, "F", "Cochin"),
        (8, "Manish", 35, "M", "Hyderabad")
        ]

columns = ["id", "name", "age", "gender", "city"]

df1 = spark.createDataFrame(data, columns)
df1.display()

# COMMAND ----------

# First DataFrame
data = [(9, "Ananth", 22, "F", "Bangalore"),
        (10, "Bole", 31, "M", "Vizak"),
        (11, "Balu", 24, "F", "Delhi"),
        (12, "Farid", 28, "M", "Nasik"),
        (13, "Sony", 29, "F", "Amaravati"),
        (14, "Nitin", 35, "M", "Pune"),
        (15, "Praveen", 26, "F", "Cochin"),
        (16, "Swaroop", 37, "M", "Hyderabad")
        ]

columns = ["id", "name", "age", "gender", "city"]

df2 = spark.createDataFrame(data, columns)
df2.display()

# COMMAND ----------

df_union_same = df1.unionByName(df2)
df_union_same.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Different Column Order**

# COMMAND ----------

# First DataFrame
data = [("Alekhya", 9, "F", 32, "New York"),
        ("Yash", 10, "M", 35, "Chicago"),
        ("Firoj", 11, "M", 37, "Delhi"),
        ("Gowthami", 12, "F", 39, "Nasik"),
        ("Anandi", 13, "F", 41, "Kolkatta"),
        ("Manohar", 14, "M", 43, "Pune"),
        ("Deepti", 15, "F", 45, "Cochin"),
        ("Mohan", 16, "M", 47, "Hyderabad")
        ]

columns = ["name", "id", "gender", "age", "city"]

df3 = spark.createDataFrame(data, columns)
df3.display()

# COMMAND ----------

# unionByName() matches by column names, so order doesn’t matter
df_union_diff = df1.unionByName(df3)

df_union_diff.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Different schemas**
# MAGIC - allowMissingColumns=True

# COMMAND ----------

# First DataFrame
data = [(1, "Andrew", 25, "F", "New York", "sony"),
        (2, "Bibin", 30, "M", "Chicago", "iphone"),
        (3, "Alex", 22, "F", "Delhi", "bpl"),
        (4, "Chetan", 23, "M", "Nasik", "bmw"),
        (5, "Albert", 27, "F", "Kolkatta", "vimson"),
        (6, "Mansoor", 32, "M", "Pune", "samsung"),
        (7, "Diliph", 29, "F", "Cochin", "snowflake"),
        (8, "Manish", 35, "M", "Hyderabad", "azure")
        ]

columns = ["id", "name", "age", "gender", "city", "product"]

df5 = spark.createDataFrame(data, columns)
df5.display()

# COMMAND ----------

# unionByName with allowMissingColumns=True fills missing columns with null
df_union_diff = df1.unionByName(df5, allowMissingColumns=True)
df_union_diff.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Union Multiple DataFrames**

# COMMAND ----------

# Use reduce() to union multiple DataFrames safely
from functools import reduce

dfs = [df1, df2, df5]
df_union = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **6) Union of two DataFrames with few common columns**

# COMMAND ----------

# First DataFrame (3 columns)
df1_comm = spark.createDataFrame(
    [
        (1, 2, 3),
        (10, 20, 30),
        (100, 200, 300)
    ],
    ["col0", "col1", "col2"]
)

# Second DataFrame (4 columns, some overlap with df1)
df2_comm = spark.createDataFrame(
    [
        (4, 5, 6, 7),
        (40, 50, 60, 70),
        (400, 500, 600, 700)
    ],
    ["col1", "col2", "col3", "col4"]
)

# Union by name with allowMissingColumns=True
df_union = df1_comm.unionByName(df2_comm, allowMissingColumns=True)

# Show final result
df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **7) Union of two DataFrames with completely different columns**

# COMMAND ----------

df1_dff_compl = spark.createDataFrame([[0, 1, 2], [3, 4, 5], [8, 9, 3], [10, 13, 15]], ["col0", "col1", "col2"])
df2_dff_compl = spark.createDataFrame([[3, 4, 5], [12, 14, 17], [20, 22, 25], [31, 35, 39]], ["col3", "col4", "col5"])

df1_dff_compl.unionByName(df2_dff_compl, allowMissingColumns=True).display()