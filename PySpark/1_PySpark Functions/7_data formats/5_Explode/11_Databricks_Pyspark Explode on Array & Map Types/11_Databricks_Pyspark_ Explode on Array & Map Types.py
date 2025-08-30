# Databricks notebook source
# MAGIC %md
# MAGIC #### Explode: 
# MAGIC    - Explode function is used to **convert** collection columns **(List, Array & Map) to rows**.
# MAGIC    - When an **array** is passed to explode function, it creates a **new row for each element in array**.
# MAGIC    - When a **map** is passed, it creates **two new columns one for key and one for value** and each element in map split into the rows. If the **array or map is NULL**, that **row is eliminated**.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, explode
import pyspark.sql.functions as f

# COMMAND ----------

help(explode)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Create dataframe with array column
# MAGIC - Explode(Array Type)

# COMMAND ----------

data = [(1, "Suresh", [".net", "Python", "Spark", "Azure"]),
        (2, "Ramya", ["java", "PySpark", "AWS"]),
        (3, "Apurba", ["C", "SAP", "Mainframes"]),
        (4, "Pranitha", ["COBOL", "DEVOPS"]),
        (5, "Sowmya", ["ABAP"])]

schema = ["id", "Name", "skills"]

df = spark.createDataFrame(data=data, schema=schema)
df.show(truncate=False)
df.printSchema()
print("Number of Rows:", df.count())

# COMMAND ----------

df = df.withColumn("New-Skills", explode(col('skills')))
display(df)
df.printSchema()

# COMMAND ----------

data1 = [(1, "Suresh", [".net", "Python", "Spark", "Azure"]),
         (2, "Ramya", ["java", "PySpark", "AWS"]),
         (3, "Rakesh", ["ADF", "SQL", None, "GCC"]),
         (4, "Apurba", ["C", "SAP", "Mainframes"]),
         (5, "Pranitha", ["COBOL", "DEVOPS"]),
         (6, "Sowmya", ["ABAP"]),
         (7, "Anand", None),
         (8, "Sourabh", [])]

schema1 = ["id", "Name", "skills"]

df1 = spark.createDataFrame(data=data1, schema=schema1)
df1.show(truncate=False)
df1.printSchema()
print("Number of Rows:", df1.count())

# COMMAND ----------

df1 = df1.withColumn("New-Skills", explode(col('skills')))
display(df1)
df1.printSchema()

# COMMAND ----------

df1.withColumn("New-Skills", explode(col('skills'))).select("id", "Name", "New-Skills").show()

# COMMAND ----------

# using select method
df1.select("id", "Name", explode(col("skills")).alias("New-Skills")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Create dataframe with map column
# MAGIC - Explode(MapType)

# COMMAND ----------

data2 = [('Sam', {'Car':'Baleno', 'Bike':'Honda', 'Office':'EcoSpace', 'Technology':'Azure'}),
         ('Krishna', {'Car':'Santro', 'Bike':'RoyalEnfield', 'Office':'Bharathi', 'Technology':'AWS'}),
         ('Arijit', {'Car':'Etios', 'Bike':'BMW', 'Office':'EcoWorld'}),
         ('Swamy', {'Car':'Swift', 'Bike':'TVS'}),
         ('Senthil', None),
         ("Anand", {})]

schema2 = ['EmpName', 'EmpDetails']

df2 = spark.createDataFrame(data=data2, schema=schema2)
display(df2)
df2.show(truncate=False)
df2.printSchema()

# COMMAND ----------

df2 = df2.select(df2.EmpName, df2.EmpDetails, explode(df2.EmpDetails))
display(df2)
df2.printSchema()

# COMMAND ----------


