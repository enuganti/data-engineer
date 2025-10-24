# Databricks notebook source
# MAGIC %md
# MAGIC #### **exceptAll**
# MAGIC
# MAGIC - The exceptAll function in PySpark is used to find the **difference between two DataFrames** while preserving `duplicates`. This means that it returns all the rows that **exist** in the **first DataFrame** but **do not appear** in the **second DataFrame**, even if there are **duplicate rows**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Use Cases**
# MAGIC
# MAGIC **Data Validation:**
# MAGIC - When performing data validation `between two datasets`, you can use exceptAll to `identify discrepancies and missing records`, even if `duplicates` exist.
# MAGIC
# MAGIC **Data Cleansing:**
# MAGIC - During data cleansing processes, you may want to find and `remove duplicates or redundant records`. exceptAll can help identify such records.
# MAGIC
# MAGIC **Data Synchronization:**
# MAGIC - When dealing with data synchronization between `different data sources or systems`, exceptAll can assist in identifying changes or discrepancies.
# MAGIC
# MAGIC **Data Quality Monitoring:**
# MAGIC - For monitoring data quality in a `streaming or batch processing` pipeline, exceptAll can help detect `anomalies and inconsistencies`.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      DataFrame.exceptAll(other)
# MAGIC
# MAGIC **DataFrame:** The source DataFrame from which you want to find the difference.
# MAGIC
# MAGIC **other:** The DataFrame you want to compare against.

# COMMAND ----------

# MAGIC %md
# MAGIC **CASE 01:** 
# MAGIC - exceptAll on two dataframes

# COMMAND ----------

# How to find all the orders that exist in df1 but do not appear in df2
# Create two DataFrames
data1 = [("Ramesh", "ADF", "Grade1", 20, 5),
         ("Kamal", "ADB", "Grade2", 25, 8),
         ("Bibin", "SQL", "Grade3", 28, 3),
         ("Bharath", "Git", "Grade4", 32, 5),
         ("Ramesh", "ADF", "Grade1", 35, 2),
         ("Ramesh", "ADF", "Grade1", 38, 6),
         ("Bibin", "SQL", "Grade3", 36, 4),
         ("Bibin", "SQL", "Grade3", 23, 7),]

data2 = [("Ramesh", "ADF", "Grade1", 20, 5),
         ("Bibin", "SQL", "Grade3", 28, 3)]

columns = ["Customer", "Tech", "Level", "Age", "Experience"]

df1 = spark.createDataFrame(data1, columns)
df2 = spark.createDataFrame(data2, columns)

display(df1)
display(df2)

# COMMAND ----------

# Use exceptAll to find the difference
result = df1.exceptAll(df2)

# Show the result
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC **CASE 02:** 
# MAGIC - exceptAll on two dataframes with required columns

# COMMAND ----------

# Assuming df1 and df2 are your DataFrames
columns_to_compare = ["Customer", "Tech", "Level"]

# Select the specific columns from each DataFrame
df1_selected = df1.select(columns_to_compare)
df2_selected = df2.select(columns_to_compare)

# Use exceptAll to find rows in df1 that are not in df2
result_df = df1_selected.exceptAll(df2_selected)

# Display the result
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **CASE 03:** 
# MAGIC - exceptAll on three dataframes

# COMMAND ----------

df1 = spark.createDataFrame([(1, "apple"), (2, "banana"), (4, "grape"), (5, "melon"), (3, "orange")], ["id", "fruit"])
df2 = spark.createDataFrame([(1, "apple"), (2, "banana")], ["id", "fruit"])
df5 = spark.createDataFrame([(4, "grape"), (5, "melon"), (6, "watermelon")], ["id", "fruit"])

# Find differences between df1 and df2, then find differences between that result and df5
# To find rows in df1 not in df2, and then further filter those results by removing rows that are also in df5
df1_df2 = df1.exceptAll(df2)
display(df1_df2)

result = df1.exceptAll(df2).exceptAll(df5)
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC **CASE 04**
# MAGIC - How to return a `new DataFrame` that `exist` in one DataFrame but `not` in the other DataFrame.

# COMMAND ----------

# DBTITLE 1,Read dataframe Rev01
df_rev01 = spark.read.csv("/FileStore/tables/exceptAll_rev01.csv", header=True, inferSchema=True)
display(df_rev01.limit(10))

# COMMAND ----------

# DBTITLE 1,Read dataframe Rev02
df_rev02 = spark.read.csv("/FileStore/tables/exceptAll_rev02.csv", header=True, inferSchema=True)
display(df_rev02.limit(10))

# COMMAND ----------

print("No of Rows in Rev01:", df_rev01.count())
print("No of distinct Rows in Rev01:", df_rev01.distinct().count())

print("\nNo of Rows in Rev02:", df_rev02.count())
print("No of distinct Rows in Rev02:", df_rev02.distinct().count())

# COMMAND ----------

# Use exceptAll to find the difference
New_Records = df_rev01.exceptAll(df_rev02)

# Show the result
display(New_Records)
