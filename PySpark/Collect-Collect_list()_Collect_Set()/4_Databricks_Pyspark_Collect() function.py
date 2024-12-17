# Databricks notebook source
# MAGIC %md
# MAGIC #### **Collect()**: Retrieve data from DataFrame
# MAGIC
# MAGIC - collect() is an **action** that returns the **entire dataset** in an **Array** to the **Driver**.

# COMMAND ----------

# MAGIC %md
# MAGIC - **collect()** is an `action` hence it **doesn't return a Dataframe** instead, it returns `data in an Array to the Driver`. Once the data is in an array, you can use `python for loop` to process it further.
# MAGIC
# MAGIC - collect() is used to retrieve the action output when you have very **small result set** and calling collect() on an RDD/Dataframe with **bigger result set** causes **out of memory** as it returns the **entire dataset (from all workers) to the driver** hence we should avoid calling collect() on a larger dataset.
# MAGIC
# MAGIC - Collect() is the function, operation for RDD or Dataframe that is used to retrieve the data from the Dataframe. It is used useful in retrieving all the elements of the row from each partition in an RDD and brings that over the driver node/program.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Loop Array in Python**
# MAGIC **Collect():** Returns data in an Array to the Driver.

# COMMAND ----------

# DBTITLE 1,create sample dataframe
data = [("Finance", 10, "Manager"), ("Marketing", 20, "Sr.Manager"), ("Sales", 30, "Representative"), ("IT", 40, "Software")]
schema = ["dept_name", "dept_id", "Designation"]

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# DBTITLE 1,Returns the entire dataset in an Array
# Returns the entire dataset in an Array
# AttributeError: 'list' object has no attribute 'display' (df1.display())
# collect() is an action hence it does not return a DataFrame instead, it returns data in an Array to the driver.
# retrieves all elements in a DataFrame as an Array of Row type to the driver node
df_collect = df.collect()
df_collect

# COMMAND ----------

# DBTITLE 1,Loop Entire dataframe
for i in df_collect:
    print(i)

# COMMAND ----------

# MAGIC %md
# MAGIC      for i in df_collect:
# MAGIC          print(i['dept_name'])
# MAGIC              (or)
# MAGIC      for i in df_collect:
# MAGIC          print(i[0])

# COMMAND ----------

# DBTITLE 1,Iterate First Column (dept_name)
for i in df_collect:
    print(i[0])

# COMMAND ----------

# MAGIC %md
# MAGIC      for i in df_collect:
# MAGIC          print(i['dept_id'])
# MAGIC             (or)
# MAGIC      for i in df_collect:
# MAGIC          print(i[1])

# COMMAND ----------

# DBTITLE 1,Iterate Second Column (dept_id)
for i in df_collect:
    print(i[1])

# COMMAND ----------

# MAGIC %md
# MAGIC      for i in df_collect:
# MAGIC          print(i['Designation'][0])
# MAGIC              (or)
# MAGIC      for i in df_collect:
# MAGIC          print(i[2])

# COMMAND ----------

# DBTITLE 1,Iterate Second Column (Designation)
for i in df_collect:
    print(i[2])

# COMMAND ----------

# MAGIC %md
# MAGIC **2) collect()[0][0]**

# COMMAND ----------

dept = [("Finance",10,"Manager"), ("Marketing",20,"Sr.Manager"), ("Sales",30,"Representative"), ("IT",40,"Software")]
deptColumns = ["dept_name", "dept_id", "Designation"]

df = spark.createDataFrame(data=dept, schema = deptColumns)
df.show(truncate=False)

# COMMAND ----------

# collect() is an action hence it does not return a DataFrame instead, it returns data in an Array to the driver.
# retrieves all elements in a DataFrame as an Array of Row type to the driver node
dataCollect = df.collect()
print(dataCollect)

# COMMAND ----------

# MAGIC %md
# MAGIC      # returns the first element in an array (1st row)
# MAGIC      df.collect()[0]
# MAGIC
# MAGIC      # returns the second element in an array (2nd row)
# MAGIC      df.collect()[1]
# MAGIC
# MAGIC      # returns the third element in an array (3rd row)
# MAGIC      df.collect()[2]
# MAGIC
# MAGIC      # returns the fourth element in an array (4th row)
# MAGIC      df.collect()[3]

# COMMAND ----------

# DBTITLE 1,1st row
# returns the first element in an array (1st row)
df.collect()[0]

# COMMAND ----------

# DBTITLE 1,2nd row
# returns the second element in an array (2nd row)
df.collect()[1]

# COMMAND ----------

# DBTITLE 1,3rd row
# returns the third element in an array (3rd row)
df.collect()[2]

# COMMAND ----------

# DBTITLE 1,4th row
# returns the fourth element in an array (4th row)
df.collect()[3]

# COMMAND ----------

# MAGIC %md
# MAGIC      # returns the value of the first row & 1st column
# MAGIC      df.collect()[0][0]
# MAGIC
# MAGIC      # returns the value of the first row & 2nd column
# MAGIC      df.collect()[0][1]
# MAGIC
# MAGIC      # returns the value of the first row & 3rd column
# MAGIC      df.collect()[0][2]

# COMMAND ----------

# DBTITLE 1,1st row & 1st column
# returns the value of the first row & 1st column
df.collect()[0][0]

# COMMAND ----------

# DBTITLE 1,1st row & 2nd column
# returns the value of the first row & 2nd column
df.collect()[0][1]

# COMMAND ----------

# DBTITLE 1,1st row & 3rd column
# returns the value of the first row & 3rd column
df.collect()[0][2]

# COMMAND ----------

# MAGIC %md
# MAGIC      # returns the value of the 2nd row & 1st column
# MAGIC      df.collect()[1][0]
# MAGIC
# MAGIC      # returns the value of the 2nd row & 2nd column
# MAGIC      df.collect()[1][1]
# MAGIC
# MAGIC      # returns the value of the 2nd row & 3rd column
# MAGIC      df.collect()[1][2]

# COMMAND ----------

# DBTITLE 1,2nd row & 1st column
# returns the value of the 2nd row & 1st column
df.collect()[1][0]

# COMMAND ----------

# DBTITLE 1,2nd row & 2nd column
# returns the value of the 2nd row & 2nd column
df.collect()[1][1]

# COMMAND ----------

# DBTITLE 1,2nd row & 3rd column
# returns the value of the 2nd row & 3rd column
df.collect()[1][2]

# COMMAND ----------

# MAGIC %md
# MAGIC      # returns the value of the 3rd row & 1st column
# MAGIC      df.collect()[2][0]
# MAGIC
# MAGIC      # returns the value of the 3rd row & 2nd column
# MAGIC      df.collect()[2][1]
# MAGIC
# MAGIC      # returns the value of the 3rd row & 3rd column
# MAGIC      df.collect()[2][2]

# COMMAND ----------

# DBTITLE 1,3rd row & 1st column
# returns the value of the 3rd row & 1st column
df.collect()[2][0]

# COMMAND ----------

# DBTITLE 1,3rd row & 2nd column
# returns the value of the 3rd row & 2nd column
df.collect()[2][1]

# COMMAND ----------

# DBTITLE 1,3rd row & 3rd column
# returns the value of the 3rd row & 3rd column
df.collect()[2][2]

# COMMAND ----------

# MAGIC %md
# MAGIC      # returns the value of the 4th row & 1st column
# MAGIC      df.collect()[3][0]
# MAGIC
# MAGIC      # returns the value of the 4th row & 2nd column
# MAGIC      df.collect()[3][1]
# MAGIC
# MAGIC      # returns the value of the 4th row & 3rd column
# MAGIC      df.collect()[3][2]

# COMMAND ----------

# DBTITLE 1,4th row & 1st column
# returns the value of the 4th row & 1st column
df.collect()[3][0]

# COMMAND ----------

# DBTITLE 1,4th row & 2nd column
# returns the value of the 4th row & 2nd column
df.collect()[3][1]

# COMMAND ----------

# DBTITLE 1,4th row & 3rd column
# returns the value of the 4th row & 3rd column
df.collect()[3][2]

# COMMAND ----------

# MAGIC %md
# MAGIC **3) df.select("column").collect()[0][0]**

# COMMAND ----------

# DBTITLE 1,first column
print(df.select("dept_name").collect())

# COMMAND ----------

# MAGIC %md
# MAGIC      # returns the first element in an array (1st row)
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[0])
# MAGIC
# MAGIC      # returns the second element in an array (2nd row)
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[1])
# MAGIC
# MAGIC      # returns the third element in an array (3rd row)
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[2])
# MAGIC
# MAGIC      # returns the fourth element in an array (4th row)
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[3])

# COMMAND ----------

# DBTITLE 1,select: 1st row
# returns the first element in an array (1st row)
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[0])

# COMMAND ----------

# DBTITLE 1,select: 2nd row
# returns the second element in an array (2nd row)
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[1])

# COMMAND ----------

# DBTITLE 1,select: 3rd row
# returns the third element in an array (3rd row)
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[2])

# COMMAND ----------

# DBTITLE 1,select: 4th row
# returns the fourth element in an array (4th row)
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[3])

# COMMAND ----------

# MAGIC %md
# MAGIC      # returns the value of the first row & 1st column
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[0][0])
# MAGIC
# MAGIC      # returns the value of the first row & 2nd column
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[0][1])
# MAGIC
# MAGIC      # returns the value of the first row & 3rd column
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[0][2])

# COMMAND ----------

# DBTITLE 1,1st row & 1st column
# returns the value of the first row & 1st column
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[0][0])

# COMMAND ----------

# DBTITLE 1,1st row & 2nd column
# returns the value of the first row & 2nd column
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[0][1])

# COMMAND ----------

# DBTITLE 1,1st row & 3rd column
# returns the value of the first row & 3rd column
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[0][2])

# COMMAND ----------

# MAGIC %md
# MAGIC      # returns the value of the second row & 1st column
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[1][0])
# MAGIC
# MAGIC      # returns the value of the second row & 2nd column
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[1][1])
# MAGIC
# MAGIC      # returns the value of the second row & 3rd column
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[1][2])

# COMMAND ----------

# DBTITLE 1,2nd row & 1st column
# returns the value of the second row & 1st column
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[1][0])

# COMMAND ----------

# DBTITLE 1,2nd row & 2nd column
# returns the value of the second row & 2nd column
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[1][1])

# COMMAND ----------

# DBTITLE 1,2nd row & 3rd column
# returns the value of the second row & 3rd column
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[1][2])

# COMMAND ----------

# MAGIC %md
# MAGIC      # returns the value of the 3rd row & 1st column
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[2][0])
# MAGIC
# MAGIC      # returns the value of the 3rd row & 2nd column
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[2][1])
# MAGIC
# MAGIC      # returns the value of the 3rd row & 3rd column
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[2][2])

# COMMAND ----------

# DBTITLE 1,3rd row & 1st column
# returns the value of the 3rd row & 1st column
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[2][0])

# COMMAND ----------

# DBTITLE 1,3rd row & 2nd column
# returns the value of the 3rd row & 2nd column
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[2][1])

# COMMAND ----------

# DBTITLE 1,3rd row & 3rd column
# returns the value of the 3rd row & 3rd column
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[2][2])

# COMMAND ----------

# MAGIC %md
# MAGIC      # returns the value of the 4th row & 1st column
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[3][0])
# MAGIC
# MAGIC      # returns the value of the 4th row & 2nd column
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[3][1])
# MAGIC
# MAGIC      # returns the value of the 4th row & 3rd column
# MAGIC      print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[3][2])

# COMMAND ----------

# DBTITLE 1,4th row & 1st column
# returns the value of the 4th row & 1st column
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[3][0])

# COMMAND ----------

# DBTITLE 1,4th row & 2nd column
# returns the value of the 4th row & 2nd column
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[3][1])

# COMMAND ----------

# DBTITLE 1,4th row & 3rd column
# returns the value of the 4th row & 3rd column
print(df.select(['dept_name', 'dept_id', 'Designation']).collect()[3][2])
