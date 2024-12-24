# Databricks notebook source
# MAGIC %md
# MAGIC **PROBLEM STATEMENT**
# MAGIC
# MAGIC - Suppose you have two dataframe df1 and df2 , both have below columns :-
# MAGIC       
# MAGIC       df1 =>  id, name, mobno
# MAGIC       df2 => id, pincode, address, city
# MAGIC
# MAGIC  - After joining both the dataframe on the basis of key i.e **id**, while  selecting **id,name,mobno,pincode, address, city**
# MAGIC  and you are getting an **error ambiguous** column id. How would you resolve it ?

# COMMAND ----------

from pyspark.sql.functions import col

# Sample data for df1 and df2
data1 = [(1, "Alice", "123456"), (2, "Bob", "789012")]
data2 = [(1, "12345", "123 Main St", "CityA"), (2, "67890", "456 Elm St", "CityB")]

columns1 = ["id", "name", "mobno"]
columns2 = ["id", "pincode", "address", "city"]

df1 = spark.createDataFrame(data1, columns1)
display(df1)

df2 = spark.createDataFrame(data2, columns2)
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Ambiguous error**

# COMMAND ----------

# MAGIC %md
# MAGIC      join_df = df1.join(df2, df1["id"] == df2["id"], how="inner")
# MAGIC                              (or)
# MAGIC      join_df_amb = df1.join(df2, col("id") == col("id"), how="inner")
# MAGIC                              (or)
# MAGIC      joined_df = df1.join(df2, on="id", how="inner")

# COMMAND ----------

join_df = df1.join(df2, df1["id"] == df2["id"], how="inner")
display(join_df)

# COMMAND ----------

# Select specific columns with aliases
result_df = join_df.select(col("id"),
                                    col("name"),
                                    col("mobno"),
                                    col("pincode"),
                                    col("address"),
                                    col("city"))
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**

# COMMAND ----------

join_df1 = df1.join(df2, on="id", how="inner")
display(join_df1)

# COMMAND ----------

# Select specific columns with aliases
result_df1 = join_df1.select(col("id"),
                            col("name"),
                            col("mobno"),
                            col("pincode"),
                            col("address"),
                            col("city"))
display(result_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 02**

# COMMAND ----------

# Rename one of the 'id' columns to avoid ambiguity
# For example, renaming 'id' in df1 to 'df1_id'
df3 = df1.withColumnRenamed('id', 'id_df1')
display(df3)

df4=df2
display(df4)

# COMMAND ----------

# Join the DataFrames on 'id'
joined_df1 = df3.join(df4, df3["id_df1"] == df4["id"], how="inner")
display(joined_df1)

# COMMAND ----------

# Select columns from both DataFrames
result_df2 = joined_df1.select('id', 'name', 'mobno', 'pincode', 'address', 'city')

# Show the result
display(result_df2)
