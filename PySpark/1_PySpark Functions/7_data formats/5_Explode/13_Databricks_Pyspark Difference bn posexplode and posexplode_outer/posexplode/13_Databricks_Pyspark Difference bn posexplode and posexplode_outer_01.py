# Databricks notebook source
# MAGIC %md
# MAGIC #### **posexplode**
# MAGIC - It is used in the PySpark data model to explode an **array or map** related **columns to rows**. 
# MAGIC - It creates a **row for each element** in the array and creates two columns **pos** to hold the **position of the array element** and the **col** to hold the **actual array value**.
# MAGIC - When the input column is **map**, posexplode function creates 3 columns **pos** to hold the **position of the map element**, **key and value** columns.
# MAGIC - If array is **NULL** then that row is **Ignored / Eliminated**.

# COMMAND ----------

# MAGIC %md
# MAGIC - Returns a **new row** for **each element** in the given **array**.
# MAGIC - when **array or map** is passed, creates **positional column** for each element.

# COMMAND ----------

from pyspark.sql.functions import col, explode, posexplode, split
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC **Array Type**

# COMMAND ----------

data = [(1, "Suresh", [".net", "Python", "Spark", "Azure"]),
        (2, "Ramya", ["java", "PySpark", "AWS"]),
        (3, "Rakesh", ["ADF", "SQL", None, "GCC"]),
        (4, "Apurba", ["C", "SAP", None]),
        (5, "Pranitha", ["COBOL", "DEVOPS"]),
        (6, "Sowmya", ["ABAP", None]),
        (7, "Anand", None),
        (8, "Sourabh", [])]
schema = ["id", "Name", "skills"]

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
display(df)
df.printSchema()
print("Number of Rows:", df.count())

# COMMAND ----------

display(df.select(df.id, df.Name, posexplode(df.skills)))

# COMMAND ----------

# MAGIC %md
# MAGIC **Map Type**

# COMMAND ----------

data1 = [('Raja', {'TV':'LG', 'Refrigerator':'Samsung', 'Oven':'Philips', 'AC':'Voltas'}),
        ('Raghav', {'AC':'Samsung', 'Washing machine': 'LG'}),
        ('Ram', {'Grinder':'Preeti', 'TV':""}),
        ('Ramesh', {'Refrigerator':'LG', 'TV':'Croma'}),
        ('Rajesh', None)]

schema1 = ['name', 'brand']

df1 = spark.createDataFrame(data=data1, schema=schema1)
display(df1)
df1.show(truncate=False)
df1.printSchema()
print("Number of Rows:", df1.count())

# COMMAND ----------

display(df1.select(df1.name, df1.brand, posexplode(df1.brand)))

# COMMAND ----------

# MAGIC %md
# MAGIC **.txt**

# COMMAND ----------

df2 = spark.read.csv("/FileStore/tables/posexplode.txt", sep="|", header=True, inferSchema=True)
display(df2)

# COMMAND ----------

df2 = df2.withColumn("New_Technology", split(("Technology"), ',')).select('EmpName', 'Dept', 'Technology', 'New_Technology')
display(df2)

# COMMAND ----------

df3 = df2.select("*", posexplode("New_Technology"))
display(df3)

# df3 = df2.select("*", posexplode(split(("Technology"), ',')))
# display(df3)

# COMMAND ----------

df3 = df3.withColumnRenamed("col", "CoreTechnology")\
         .withColumnRenamed("pos", "Index")\
         .drop("Technology", "New_Technology")        
display(df3)
