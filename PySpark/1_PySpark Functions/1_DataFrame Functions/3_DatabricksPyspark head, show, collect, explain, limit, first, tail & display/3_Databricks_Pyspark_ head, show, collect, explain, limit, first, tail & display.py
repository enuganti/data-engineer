# Databricks notebook source
# MAGIC %md
# MAGIC - show()
# MAGIC - display()
# MAGIC - head()
# MAGIC - tail()
# MAGIC - first()
# MAGIC - limit()
# MAGIC - top()
# MAGIC - collect()
# MAGIC - explain()

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/titanic.csv", header=True, inferSchema=True)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **show**

# COMMAND ----------

df.show(2, truncate= False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **display**
# MAGIC
# MAGIC - **display(df)** will also display the dataframe in the **tabular format**, but along with normal tabular view, we can leverage the display() function to get the different views like **tablular, pie, Area, Bar, etc.,** and download options from Databricks.
# MAGIC
# MAGIC - Dataframe.Display method in Databricks notebook fetches only 1000 rows by default

# COMMAND ----------

df.display()

# databricks specific function (this function won't work in SPARK)
#display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **head**

# COMMAND ----------

# Returns the first Row
#df.head()

# to display top n rows in the dataframe
df.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **tail**

# COMMAND ----------

# Returns Last N rows
# to return last n rows in the dataframe
df.tail(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **first**

# COMMAND ----------

# Returns the first Row (display first row of the dataframe)
df.first()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **limit**

# COMMAND ----------

# Returns Top N rows
#df.limit(3)

display(df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **top**

# COMMAND ----------

# used to select top n rows

# select top 2 rows
#print(df.take(2))

# select top 4 rows
print(df.take(4))

# select top 1 row
#print(df.take(1))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **collect**

# COMMAND ----------

# Returns all dataset
data = df.collect()
print(data)

# COMMAND ----------

print(data[0])

# COMMAND ----------

#print(data[0])
#print(data[0][0])
print(data[0][5])

# COMMAND ----------

# select first row
print(df.select(['Name',
                        'Ticket',
                        'Pclass']).collect()[0])

# COMMAND ----------

# select third row
print(df.select(['Name',
                        'Ticket',
                        'Pclass']).collect()[2])

# COMMAND ----------

# select forth row
print(df.select(['Name',
                        'Ticket',
                        'Pclass']).collect()[3])

# COMMAND ----------

# MAGIC %md
# MAGIC #### **explain**

# COMMAND ----------


df.explain()