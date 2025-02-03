# Databricks notebook source
# MAGIC %md
# MAGIC #### **Question 01**

# COMMAND ----------

# MAGIC %md
# MAGIC      We have multiple columns in data as below:
# MAGIC             column1   column2    column 3
# MAGIC
# MAGIC      How to add new column `(column3)*2` in existing data set?
# MAGIC            output:
# MAGIC              column1   column2     column 3   new_column

# COMMAND ----------

# DBTITLE 1,Create Table
from pyspark.sql.functions import col

# Sample data
data = [(1, 2, 3),
        (4, 5, 6),
        (7, 8, 9)]

# Define schema
columns = ["column1", "column2", "column3"]

# Create DataFrame
df_q3 = spark.createDataFrame(data, columns)
display(df_q3)

# COMMAND ----------

# DBTITLE 1,Adding New Column
# Add new column which is (column3) * 2
df_with_new_column = df_q3.withColumn("new_column", col("column3") * 2)

# Display the DataFrame
display(df_with_new_column)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 02**

# COMMAND ----------

# MAGIC %md
# MAGIC       We have column as below order
# MAGIC             item_id       c1        c2
# MAGIC                1        [a,b,c]     d
# MAGIC       
# MAGIC       How to separate `c1` column as individual as below?
# MAGIC             item_id      c1        c2       c3        c4
# MAGIC                1         a         b        c         d

# COMMAND ----------

# Sample data
data = [
    (1, ["a", "b", "c"], "d")
]

# Define schema
columns = ["item_id", "c1", "c2"]

# Create DataFrame
df_Q4 = spark.createDataFrame(data, columns)
display(df_Q4)

# COMMAND ----------

from pyspark.sql.functions import split, col, explode, max, size

# COMMAND ----------

# MAGIC %md
# MAGIC      # Use selectExpr to split the array into separate columns
# MAGIC      df_expr = df_Q4.selectExpr("item_id", "c1[0] as c1", "c1[1] as c2", "c1[2] as c3", "c2")
# MAGIC      display(df_expr)

# COMMAND ----------

# Split the c1 column into individual columns
df_split = df_Q4.withColumn("c1_split_0", df_Q4.c1[0]) \
                .withColumn("c1_split_1", df_Q4.c1[1]) \
                .withColumn("c1_split_2", df_Q4.c1[2]) \
                .withColumnRenamed("c2", "c4") \
                .drop('c1') \
                .withColumnRenamed("c1_split_0", "c1") \
                .withColumnRenamed("c1_split_1", "c2") \
                .withColumnRenamed("c1_split_2", "c3")

# Display the DataFrame
display(df_split)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 03**

# COMMAND ----------

# DBTITLE 1,Create Table
# Sample data
data = [(1, ["a", "b", "c"], "a"),
        (2, ["g", None, "c"], "b"),
        (3, ["m", "c"], "c"),
        (4, ["n"], "d")
]

# Define schema
columns = ["item_id", "value", "index"]

# Create DataFrame
df_Q41 = spark.createDataFrame(data, columns)
display(df_Q41)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Split Array values into seperate columns**

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**

# COMMAND ----------

df_Q41.select("item_id", df_Q41.value[0], df_Q41.value[1], df_Q41.value[2], "index").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 02**

# COMMAND ----------

# DBTITLE 1,Determine the Size of Each Array
# Determine the maximum number of elements in the 'value' column
dfsize = df_Q41.select("item_id", "value", size("value").alias('NoOfArrayElements'))
display(dfsize)

# COMMAND ----------

# DBTITLE 1,Get the Max Size of All Arrays
# max_value = dfsize.agg({"NoOfArrayElements": "max"}).collect()[0][0]
max_value = dfsize.agg(max(col("NoOfArrayElements")).alias('NoOfArrayElements')).collect()[0][0]
print(max_value)

# COMMAND ----------

# DBTITLE 1,UDF to Convert Array Elements into columns
# Function to split array into columns
def arraySplitIntoCols(df, maxElements):
    for i in range(maxElements):
        df = df.withColumn(f"new_col_{i}", df.value[i])
    return df

# COMMAND ----------

# DBTITLE 1,UDF call
# Split the 'value' column into separate columns
dfout = arraySplitIntoCols(df_Q41, max_value)
# Display the DataFrame
display(dfout)
