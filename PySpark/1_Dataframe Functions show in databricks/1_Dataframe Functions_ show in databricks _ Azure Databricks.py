# Databricks notebook source
# MAGIC %md
# MAGIC By default, PySpark displays only the **first 20 rows** and truncates each **column to 20 characters**. To change this, you can adjust the following configurations:
# MAGIC
# MAGIC - **How to show Full Data:** Use the show() method with the truncate parameter set to False:
# MAGIC      
# MAGIC        df.show(truncate=False)
# MAGIC
# MAGIC - **How to display More than 20 Rows:** If you want to display more than 20 rows, specify the number of rows as the first parameter in the show() method:
# MAGIC      
# MAGIC        df.show(n=100, truncate=False)

# COMMAND ----------

 help(df.show)

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/titanic.csv", header=True, inferSchema=True)
df.show()

# COMMAND ----------

# prints 20 records by default
df.show()

# COMMAND ----------

# Both gives same results
# df.show()
df.show(truncate=True)    

# COMMAND ----------

# show df created top 10 rows
df.show(10)

# COMMAND ----------

 # visualizing full content of the Dataframe by setting truncate to False
 # Both gives same results
 df.show(truncate=False)
 #df.show(truncate=0)

# COMMAND ----------

#df.show(truncate=1)
#df.show(truncate=5)
df.show(truncate=45)

# COMMAND ----------

# Show top 2 rows and full column contents (PySpark)
df.show(2, truncate=False)

# COMMAND ----------

# Shows top 2 rows and only 25 characters of each column (PySpark)
df.show(2, truncate=25)

# COMMAND ----------

# get total row count and pass it as argument to show
df.show(df.count())

# COMMAND ----------

df.show(df.count(), truncate=False)

# COMMAND ----------

#df.select('Name').show(truncate=False)
df.select(['Name', 'Ticket']).show(truncate=False)

# COMMAND ----------

# Shows rows vertically (one line per column value) (PySpark)
df.show(vertical=True)

# COMMAND ----------

# Both gives same results
df.show(n=3, truncate=25, vertical=True)
#df.show(3, 25, True)
