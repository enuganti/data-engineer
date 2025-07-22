# Databricks notebook source
# MAGIC %md
# MAGIC Topics covered:
# MAGIC - **schema**
# MAGIC - **count**
# MAGIC - **columns**
# MAGIC - **describe**
# MAGIC - **summary**

# COMMAND ----------

# DBTITLE 1,Read Sample dataset
df = spark.read.csv("dbfs:/FileStore/tables/titanic.csv", header=True, inferSchema=True)
display(df)
# df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) schema**

# COMMAND ----------

# MAGIC %md
# MAGIC      df.printSchema()
# MAGIC
# MAGIC      df.schema
# MAGIC      print(df.schema)
# MAGIC
# MAGIC      df.schema.fields
# MAGIC
# MAGIC      # Returns dataframe column names and data types
# MAGIC      df.dtypes

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.schema

# COMMAND ----------

print(df.schema)

# COMMAND ----------

df.schema.fields

# COMMAND ----------

# Returns dataframe column names and data types
df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) count**
# MAGIC
# MAGIC      df.count()
# MAGIC      df.distinct().show()
# MAGIC      df.distinct().count()

# COMMAND ----------

#df.count()
#df.distinct().show()
df.distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) List of the column names / Number of Columns**
# MAGIC
# MAGIC      # All gives same output
# MAGIC      df.columns
# MAGIC      df.schema.names
# MAGIC      df.schema.fieldNames()

# COMMAND ----------

df.columns

# COMMAND ----------

df.schema.names

# COMMAND ----------

df.schema.fieldNames()

# COMMAND ----------

df.columns[0]

# COMMAND ----------

for col in df.columns:
    print(col)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) Number of columns**
# MAGIC
# MAGIC      len(df.columns)
# MAGIC      len(df.dtypes)

# COMMAND ----------

#len(df.columns)
len(df.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **5) describe**

# COMMAND ----------

#df.describe().show()
df.describe(['Name']).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **6) summary**

# COMMAND ----------

df.summary().show()

# COMMAND ----------

df.summary("count", "33%", "50%", "66%").show()

# COMMAND ----------

df.summary("count", "count_distinct").show()

# COMMAND ----------

df.summary("count", "approx_count_distinct").show(truncate=False)

# COMMAND ----------

df.select("Name").summary("count", "33%", "50%", "66%").show()