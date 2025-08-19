# Databricks notebook source
# MAGIC %md
# MAGIC **How to transform String Columns to ArrayType column and flatten into individual columns?**

# COMMAND ----------

# MAGIC %md
# MAGIC      index
# MAGIC      getItem
# MAGIC      element_at
# MAGIC      Array Size

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, array, array_contains, size, element_at
from pyspark.sql.types import IntegerType, StringType, ArrayType, StructType, StructField

# COMMAND ----------

# DBTITLE 1,create dataframe
data = [("Amar,,Singh",["Java","Scala","C++"], ["Spark","Java","Azure Databricks"], [8, 9, 5, 7], "Bangalore", "Chennai", 25, 7),
        ("Ramesh,Rathode,", ["Python","PySpark","C"], ["spark sql","ADF"], [11, 3, 6, 8], "Hyderabad", "Kochin", 35, 8),
        ("Asha,,Rani", ["Devops","VB","Git"], ["ApacheSpark","Python"], [5, 6, 8, 10], "Amaravathi", "Noida", 30, 10),
        ("Rakesh,Kothur,", ["SQL","Azure","AWS"], ["PySpark","Oracle","Confluence"], [12, 6, 8, 15], "Noida", "Mumbai", 33, 5),
        ("Krishna,,Joshi", ["GCC","Visual Studio"], ["SQL","Databricks","SQL Editor"], [2, 6, 5, 8], "Delhi", "Kolkata", 28, 6),
        ("Hari,,Rani", ["Devops","VB","Git"], ["ApacheSpark","Python"], [5, 6, 8, 10], "Amaravathi", "Noida", 30, 10),
        ("Rakesh,kumar,", ["SQL","Azure","AWS"], ["PySpark","Oracle","Schema"], [12, 6, 8, 15], "luknow", "Mumbai", 33, 5),
        ("karan,,Joshi", ["AWS","Visual Studio"], ["SQL","Git","SQL Editor"], [2, 6, 5, 8], "Delhi", "Noida", 28, 6),
        ]

schema = StructType([ 
    StructField("FullName", StringType(), True), 
    StructField("LearntLanguages", ArrayType(StringType()), True), 
    StructField("ToLearnLanguages", ArrayType(StringType()), True),
    StructField("Rating", ArrayType(IntegerType()), True), 
    StructField("PresentState", StringType(), True), 
    StructField("PreviousState", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Experience", IntegerType(), True)
  ])

df2 = spark.createDataFrame(data=data, schema=schema)
df2.printSchema()
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC The below example **combines** the data from **PresentState and PreviousState** and creates a **new column states**.

# COMMAND ----------

df2.select(df2.FullName, array(df2.PresentState, df2.PreviousState).alias("States"))\
   .withColumn("Size", F.size("States")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Index**

# COMMAND ----------

df2.withColumn("Rating1", df2["Rating"][0])\
   .withColumn("Rating2", df2["Rating"][1])\
   .withColumn("Rating3", df2["Rating"][2])\
   .withColumn("Rating4", df2["Rating"][3])\
   .select("FullName", "Rating", "Rating1", "Rating2", "Rating3", "Rating4").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **getItem**

# COMMAND ----------

display(df2.withColumn("get_TLL_01", col("ToLearnLanguages").getItem(0))\
           .withColumn("get_TLL_02", col("ToLearnLanguages").getItem(1))\
           .withColumn("get_TLL_03", col("ToLearnLanguages").getItem(2))\
           .select("ToLearnLanguages", "get_TLL_01", "get_TLL_02", "get_TLL_03"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **element_at**
# MAGIC
# MAGIC - The **position is not zero** based, but **1 based index**.

# COMMAND ----------

display(df2.withColumn("elm_TLL_01", element_at("ToLearnLanguages", 1))\
           .withColumn("elm_TLL_02", element_at("ToLearnLanguages", 2))\
           .withColumn("elm_TLL_03", element_at("ToLearnLanguages", 3))\
           .withColumn("elm_TLL_04", element_at("ToLearnLanguages", -1))\
           .select("ToLearnLanguages", "elm_TLL_01", "elm_TLL_02", "elm_TLL_03", "elm_TLL_04"))

# COMMAND ----------

display(df2.withColumn("lit", element_at("ToLearnLanguages", lit(2)))\
           .select("ToLearnLanguages", "lit"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Array Size**
# MAGIC - returns the total **number of elements** in the **array column**.
# MAGIC - If your input array column is **null**, it returns **null**.

# COMMAND ----------

# MAGIC %md
# MAGIC      # 12.2 LTS (includes Apache Spark 3.3.2, Scala 2.12)
# MAGIC      from pyspark.sql.functions import size
# MAGIC
# MAGIC      # 15.4 LTS (includes Apache Spark 3.5.0, Scala 2.12)
# MAGIC      from pyspark.sql.functions import array_size

# COMMAND ----------

display(df2.withColumn("Arr_Size_01", array("PresentState", "PreviousState", size("ToLearnLanguages")))\
           .withColumn("Arr_Size_02", array("PresentState", size("LearntLanguages"), size("ToLearnLanguages")))\
           .withColumn("Arr_Size_03", array(size("Rating"), size("LearntLanguages"), size("ToLearnLanguages")))\
           .select("LearntLanguages", "ToLearnLanguages", "PresentState", "PreviousState", "Arr_Size_01", "Arr_Size_02", "Arr_Size_03"))
