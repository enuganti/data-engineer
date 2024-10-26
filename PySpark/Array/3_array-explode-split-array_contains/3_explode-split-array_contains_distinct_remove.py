# Databricks notebook source
# MAGIC %md
# MAGIC #### **Array Functions**
# MAGIC
# MAGIC 1) Explode
# MAGIC 2) Split
# MAGIC 3) Array
# MAGIC 4) Array_contains
# MAGIC 5) array_distinct
# MAGIC 6) array_remove

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, ArrayType, StructType, StructField
from pyspark.sql.functions import explode, split, array, array_contains, array_distinct, array_remove

# COMMAND ----------

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

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Explode**
# MAGIC
# MAGIC - To create a **new row for each element** in the given **array column**.

# COMMAND ----------

# DBTITLE 1,Explode Array of String elements
df_expl = df.select("FullName", explode("LearntLanguages"))
display(df_expl)

# COMMAND ----------

df_expl1 = df.select("FullName", explode("Rating"))
display(df_expl1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Split**
# MAGIC
# MAGIC - It Convert **String Column to Array**.
# MAGIC
# MAGIC - Returns an **array type** after splitting the **string column by delimiter**.
# MAGIC - The split function takes two arguments: the name of the **column** to split and the **delimiter**.
# MAGIC
# MAGIC       Convert array to string: F.concat_ws()
# MAGIC       Convert string to array: F.split()

# COMMAND ----------

# DBTITLE 1,Convert String Column to Array of String
df_spl = df.select("FullName", split("FullName",",").alias("FullNameSplit"))
display(df_spl)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Array**
# MAGIC
# MAGIC - To create a **new array column** by **merging** the data from **multiple columns**.
# MAGIC - All input columns must have the **same data type**.

# COMMAND ----------

# DBTITLE 1,Create Array column from multiple columns
df_arry = df.withColumn("Comb_Lang", array("LearntLanguages", "ToLearnLanguages"))\
            .withColumn("Age_Exp", array("Age", "Experience"))\
            .select("LearntLanguages", "ToLearnLanguages", "Comb_Lang", "Age", "Experience", "Age_Exp")
display(df_arry)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) array_contains**
# MAGIC
# MAGIC - used to check if **array column contains a value**.
# MAGIC   - **True**: If the value is **present**.
# MAGIC   - **False**: If the value is **not present**.
# MAGIC   - **null**: If the array column is **null/None**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax**
# MAGIC
# MAGIC      array_contains(array_column, value)
# MAGIC      
# MAGIC      column (str, Column): It represents a column of ArrayType
# MAGIC      value (str): It represents the value to check if it is in the array column

# COMMAND ----------

df_arr_con = df.select("FullName",\
  "LearntLanguages", array_contains(df.LearntLanguages, "SQL").alias("Knows_Python"),\
  "ToLearnLanguages", array_contains(df.ToLearnLanguages, "PySpark").alias("Knows_PySpark"),\
  "Rating", array_contains(df.Rating, 8).alias("rating"))
display(df_arr_con)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **5) array_distinct**
# MAGIC
# MAGIC - To **remove duplicate** values from **array column**.
# MAGIC
# MAGIC #### **Syntax**
# MAGIC
# MAGIC       array_distinct(column)

# COMMAND ----------

data_dup = [("Amar",["Java","Scala","C++","C++"], ["Spark","Java","Azure","Java"], [8, 9, 5, 7, 5, 8]),
            ("Ramesh", ["Python","PySpark","C","Python"], ["spark sql","ADF","ADF"], [11, 3, 6, 8, 3]),
            ("Asha", ["Devops","VB","Git","VB"], ["ApacheSpark","Python","Python"], [5, 6, 8, 10, 5, 6]),
            ("Rakesh", ["SQL","Azure","AWS","SQL"], ["PySpark","Oracle","Confluence","PySpark"], [12, 6, 8, 15, 6]),
            ("Krishna", ["GCC","Visual Studio","GCC"], ["SQL","Databricks","SQL Editor","Databricks"], [2, 6, 5, 8, 8, 6]),
            ("Hari", ["Devops","VB","Git","VB"], ["ApacheSpark","Python","Python"], [5, 6, 8, 10, 5, 6]),
            ("Rakesh", ["SQL","Azure","AWS","Azure"], ["PySpark","Oracle","Schema","PySpark"], [12, 6, 8, 15, 12, 8]),
            ("karan", ["AWS","Visual Studio","SQL","AWS"], ["SQL","Git","SQL Editor","Git"], [2, 6, 5, 8, 6, 8, 5]),
           ]

schema_dup = StructType([
  StructField("FullName", StringType(), True), 
  StructField("LearntLanguages", ArrayType(StringType()), True), 
  StructField("ToLearnLanguages", ArrayType(StringType()), True),
  StructField("Rating", ArrayType(IntegerType()), True)
  ])

df_dup = spark.createDataFrame(data=data_dup, schema=schema_dup)
display(df_dup)

# COMMAND ----------

df_dist = df_dup.withColumn("dup_Learnt", array_distinct("LearntLanguages"))\
                .withColumn("dup_ToLearn", array_distinct("ToLearnLanguages"))\
                .withColumn("dup_Rating", array_distinct("Rating"))\
                .select("LearntLanguages", "dup_Learnt", "ToLearnLanguages", "dup_ToLearn", "Rating", "dup_Rating")
display(df_dist)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **6) array_remove**
# MAGIC
# MAGIC - To **remove** particular element from the **array column**.
# MAGIC - It will **remove** all the **occurrence** of that element.
# MAGIC
# MAGIC #### **Syntax**
# MAGIC
# MAGIC      array_remove(column, value)
# MAGIC
# MAGIC - 1st parameter(column) takes a **column name** containing **array**.
# MAGIC - 2nd parameter(value) takes a **value** to **find index** of that element.     

# COMMAND ----------

data_rm = [("Amar",["Java","Scala","C++","C++"], ["Spark","Java","Azure","Python"], [8, 9, 5, 7, 5, 8, 8]),
           ("Ramesh", ["Python","PySpark","C","Python"], ["spark sql","ADF","ADF"], [11, 3, 6, 8, 3, 8]),
           ("Asha", ["Devops","VB","Git","VB"], ["ApacheSpark","Python","Python"], [5, 6, 8, 10, 5, 6, 8]),
           ("Rakesh", ["SQL","Azure","AWS","SQL"], ["PySpark","Oracle","Python","PySpark"], [12, 6, 8, 15, 6, 8]),
           ("Krishna", ["GCC","Visual Studio","GCC"], ["SQL","Databricks","Python","Databricks"], [2, 6, 5, 8, 8, 6, 8]),
           ("Hari", ["Devops","VB","Git","VB"], ["ApacheSpark","Python","Python"], [5, 6, 8, 10, 5, 6, 8]),
           ("Rakesh", ["SQL","Azure","AWS","Azure"], ["PySpark","Oracle","Schema","Python"], [12, 6, 8, 15, 12, 8, 8]),
           ("karan", ["AWS","Visual Studio","SQL","AWS"], ["SQL","Git","SQL Editor","Python"], [2, 6, 5, 8, 6, 8, 5, 8]),
          ]

schema_rm = StructType([
  StructField("FullName", StringType(), True), 
  StructField("LearntLanguages", ArrayType(StringType()), True), 
  StructField("ToLearnLanguages", ArrayType(StringType()), True),
  StructField("Rating", ArrayType(IntegerType()), True)
  ])

df_rm = spark.createDataFrame(data=data_rm, schema=schema_rm)
display(df_rm)

# COMMAND ----------

# All occurrences of element "a" were removed.
df_rem = df_rm.withColumn("rm_Learnt", array_remove("LearntLanguages", "C++"))\
              .withColumn("rm_ToLearn", array_remove("ToLearnLanguages", "Python"))\
              .withColumn("rm_Rating", array_remove("Rating", 8))\
              .select("LearntLanguages", "rm_Learnt", "ToLearnLanguages", "rm_ToLearn", "Rating", "rm_Rating")
display(df_rem)
