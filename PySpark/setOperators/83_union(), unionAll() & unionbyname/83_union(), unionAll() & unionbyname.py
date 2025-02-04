# Databricks notebook source
# MAGIC %md
# MAGIC #### **union(), unionAll() & unionByName()**
# MAGIC
# MAGIC **PySpark “2.0.0”**
# MAGIC
# MAGIC - **union()**
# MAGIC   - **Removes** the **duplicate records** from resultant dataframe until **spark version 2.0.0**. So duplicate can be removed manually by **dropDuplicates()**.
# MAGIC
# MAGIC - **unionAll()**
# MAGIC   - Same as union but **retains duplicate records** as well in resultant dataframe.
# MAGIC
# MAGIC **union():** 
# MAGIC - Combines **two DataFrames** have the **same column order and schema**.
# MAGIC
# MAGIC     - **union() and unionAll()** transformations are used to **merge two or more DataFrame’s** of the **same schema or structure**.
# MAGIC     - The output includes `all rows from both DataFrames` and **duplicates are retained**.
# MAGIC     - If schemas are `not the same it returns an error`.
# MAGIC
# MAGIC **unionAll():**
# MAGIC   - Alias for union(), behaves the same.
# MAGIC   - **unionAll()** method is **deprecated** since **PySpark “2.0.0”** version and **recommends** using the **union()** method.
# MAGIC
# MAGIC **unionByName():**
# MAGIC - Combines two DataFrames by matching **column names**, even **if column order differs**.
# MAGIC - To deal with the DataFrames of **different schemas** we need to use **unionByName()** transformation.

# COMMAND ----------

# MAGIC %md
# MAGIC **When to Use What?**
# MAGIC
# MAGIC - Use **union() or unionAll()** when **schemas and column orders** are the **same**.
# MAGIC - Use **unionByName()** when **column names** are the **same** but their **order** might be **different**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Differences**
# MAGIC
# MAGIC          |   Function    |    Same Schema	|  Same Column Order	|  Matches Column Names  |
# MAGIC          |---------------|------------------|-----------------------|------------------------|
# MAGIC          | union()	     |   ✅ Required	|   ✅ Required	        |   ❌ No Matching       |
# MAGIC          | unionAll()    |   ✅ Required    |   ✅ Required	        |   ❌ No Matching       |
# MAGIC          | unionByName() |   ✅ Required	|   ❌ Not Required	|   ✅ Matches Columns   |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      df1.union(df2)
# MAGIC      df1.unionAll(df2)
# MAGIC      df1.unionByName(df3)

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.master("local").getOrCreate()

# Get Spark version
print(spark.sparkContext.version)

# COMMAND ----------

# DBTITLE 1,dataframe 01
simpleData = [("Kiran", "Sales", "AP", 890000, 24, 35000), \
              ("Mohan", "Admin", "TN", 756000, 36, 45000), \
              ("Robert", "Marketing", "KA", 567000, 33, 35000), \
              ("Swetha", "Finance", "PNB", 598000, 26, 99000), \
              ("Kamalesh", "IT", "TS", 8946000, 31, 56000), \
              ("Mathew", "Maintenance", "KL", 667000, 28, 467000), \
              ("Santhosh", "Sales", "MH", 873000, 24, 734000),\
              ("Swetha", "Finance", "PNB", 598000, 26, 99000), \
              ("Mohan", "Admin", "TN", 756000, 36, 45000)
              ]

columns= ["employee_name", "department", "state", "salary", "age", "bonus"]

df1 = spark.createDataFrame(data = simpleData, schema = columns)
df1.printSchema()
display(df1)

# COMMAND ----------

# DBTITLE 1,dataframe 02
# Create DataFrame2
simpleData2 = [("Kailash", "Sales", "RJ", 96600, 30, 15500), \
               ("Somesh", "Finance", "UP", 88000, 22, 27800), \
               ("Jennifer", "Support", "TN", 59000, 43, 35500), \
               ("Kumar", "Marketing", "CA", 768000, 28, 945000), \
               ("Sandya", "IT", "PNB", 789000, 37, 678900), \
               ("Swaroop", "Admin", "KL", 679000, 24, 478000), \
               ("Joseph", "Finance", "DL", 789000, 29, 456700), \
               ("Rashi", "Maintenance", "TS", 467800, 23, 872300), \
               ("Krishna", "Backend", "AP", 945670, 39, 435000),\
               ("Sandya", "IT", "PNB", 789000, 37, 678900), \
               ("Swaroop", "Admin", "KL", 679000, 24, 478000)
               ]
columns2= ["employee_name", "department", "state", "salary", "age", "bonus"]

df2 = spark.createDataFrame(data = simpleData2, schema = columns2)

df2.printSchema()
display(df2)

# COMMAND ----------

# DBTITLE 1,dataframe 04
# dataframe with different order of columns as compared to df1
simpleData4 = [("Kailash", 96600, "Sales", 30, "RJ", 15500), \
               ("Somesh", 88000, "Finance", 22, "UP", 27800), \
               ("Jennifer", 59000, "Support", 43, "TN", 35500), \
               ("Kumar", 768000, "Marketing", 28, "CA", 945000), \
               ("Sandya", 789000, "IT", 37, "PNB", 678900), \
               ("Swaroop", 679000, "Admin", 24, "KL", 478000), \
               ("Joseph", 789000, "Finance", 29, "DL", 456700), \
               ("Rashi", 467800, "Maintenance", 23, "TS", 872300), \
               ("Krishna", 945670, "Backend", 39, "AP", 435000)
               ]
columns4 = ["employee_name", "salary", "department", "age", "state", "bonus"]

df4 = spark.createDataFrame(data = simpleData4, schema = columns4)

df4.printSchema()
display(df4)

# COMMAND ----------

# DBTITLE 1,dataframe 05
# dataframe with different order of columns and count as compared to df1
simpleData5 = [("Kailash", 96600, "Sales", 30, "RJ", 15500, 12345), \
               ("Somesh", 88000, "Finance", 22, "UP", 27800, 67890), \
               ("Jennifer", 59000, "Support", 43, "TN", 35500, 14789), \
               ("Kumar", 768000, "Marketing", 28, "CA", 945000, 98765), \
               ("Sandya", 789000, "IT", 37, "PNB", 678900, 85432), \
               ("Swaroop", 679000, "Admin", 24, "KL", 478000, 74321), \
               ("Joseph", 789000, "Finance", 29, "DL", 456700, 45980), \
               ("Rashi", 467800, "Maintenance", 23, "TS", 872300, 517132), \
               ("Krishna", 945670, "Backend", 39, "AP", 435000, 560103)
               ]
columns5 = ["employee_name", "salary", "department", "age", "state", "bonus", "pincode"]

df5 = spark.createDataFrame(data = simpleData5, schema = columns5)

df5.printSchema()
display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Merge two or more DataFrames using union**
# MAGIC - union() method merges **two DataFrames** and returns the new DataFrame with **all rows** from two Dataframes regardless of **duplicate data**.
# MAGIC
# MAGIC       same column names
# MAGIC       same order
# MAGIC       same column count
# MAGIC

# COMMAND ----------

# DBTITLE 1,same column names, order and count
# union() to merge two DataFrames has same column names, order and count
# df1: "employee_name", "department", "state", "salary", "age", "bonus"
# df2: "employee_name", "department", "state", "salary", "age", "bonus"
unionDF = df1.union(df2)
unionDF.display()

# COMMAND ----------

display(unionDF.dropDuplicates())

# COMMAND ----------

# DBTITLE 1,rename column of df2
df2_col_re = df2.withColumnRenamed("bonus", "bonus_new")
display(df2_col_re)
unionDFNew = df1.union(df2_col_re)
unionDFNew.display()

# COMMAND ----------

# DBTITLE 1,Rename multiple columns of df2
df2_mltcol_re = df2.withColumnRenamed("employee_name", "EName")\
                   .withColumnRenamed("department", "dept")\
                   .withColumnRenamed("state", "country")\
                   .withColumnRenamed("salary", "commission")\
                   .withColumnRenamed("bonus_new", "age")\
                   .withColumnRenamed("age", "bonus")
                   
unionDFMult = df1.union(df2_mltcol_re)
unionDFMult.display()

# COMMAND ----------

# DBTITLE 1,Adding new column to df2
df3 = df2.withColumn("hike", df2.bonus*100)
display(df3)

# COMMAND ----------

# DBTITLE 1,different column count
# union() to merge two DataFrames of different count
# df1 and df2 has same column names and order but with one extra column in df2
# df1: "employee_name", "department", "state", "salary", "age", "bonus"
# df3: "employee_name", "department", "state", "salary", "age", "bonus", "pincode"
unionDF1 = df1.union(df3)
unionDF1.display()

# COMMAND ----------

# DBTITLE 1,Remove column "bonus"
df3 = df3.select("employee_name", "department", "state", "salary", "age")
display(df3)

# COMMAND ----------

# union() to merge two DataFrames of different count
# df1 and df3 has same column names and order but with one less column than df2
# df1: "employee_name", "department", "state", "salary", "age", "bonus"
# df3: "employee_name", "department", "state", "salary", "age"
unionDF2 = df1.union(df3)
unionDF2.display()

# COMMAND ----------

# DBTITLE 1,different order of columns
# union() to merge two DataFrames of different order of columns
# df1: "employee_name", "department", "state", "salary", "age", "bonus"
# df4: "employee_name", "salary", "department", "age", "state", "bonus"
unionDF3 = df1.union(df4)
unionDF3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Merge DataFrames using unionAll**
# MAGIC - DataFrame **unionAll()** method is **deprecated** since **PySpark “2.0.0”** version and recommends using the **union()** method.

# COMMAND ----------

# DBTITLE 1,union All
# unionAll() to merge two DataFrames
unionAllDF = df1.unionAll(df2)
unionAllDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Merge without Duplicates**
# MAGIC
# MAGIC - Since the union() method returns **all rows without distinct records**, we will use the distinct() function to return just one record when a **duplicate exists**.

# COMMAND ----------

# Remove duplicates after union() using distinct()
disDF = df1.union(df2).distinct()
display(disDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **3) unionByName**

# COMMAND ----------

# DBTITLE 1,unionByName
# same column names, count and order
unionByName = df1.unionByName(df2)
unionByName.display()

# COMMAND ----------

# DBTITLE 1,unionByName
# unionByName to merge two DataFrames has same column count and different order of columns
# df1: "employee_name", "department", "state", "salary", "age", "bonus"
# df4: "employee_name", "salary", "department", "age", "state", "bonus"
unionByName1 = df1.unionByName(df4)
unionByName1.display()

# COMMAND ----------

# DBTITLE 1,unionByName
# unionByName to merge two DataFrames has different column count and different order of columns
# df1: "employee_name", "department", "state", "salary", "age", "bonus"
# df5: "employee_name", "salary", "department", "age", "state", "bonus", "pincode"
unionByName2 = df1.unionByName(df5)
unionByName2.display()
