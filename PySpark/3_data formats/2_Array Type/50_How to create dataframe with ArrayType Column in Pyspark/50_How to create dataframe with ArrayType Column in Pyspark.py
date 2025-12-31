# Databricks notebook source
# MAGIC %md
# MAGIC ##### Array
# MAGIC
# MAGIC - Array function is used to create a **new column** of **array type** by **combining two columns**.
# MAGIC - All input columns must have the **same data type**.
# MAGIC - **All elements** of ArrayType should have the **same type of elements**.
# MAGIC - when we work with **json** data, it is very common to get **ArrayType columns**.  
# MAGIC - You can think of a **PySpark array** column in a similar way to a **Python list**.
# MAGIC
# MAGIC **Why Change a Column from String to Array?**
# MAGIC
# MAGIC - In PySpark, the **explode** function is used to **transform each element** of an **array** in a DataFrame column into a **separate row**. However, this function requires the column to be an **array**. If your data is in **string** format, you’ll need to **convert it to an array** before using explode.

# COMMAND ----------

# MAGIC %md
# MAGIC      How to transform Integer Column to Array Type?
# MAGIC      How to transform String Columns to Array Type?
# MAGIC      How to get size of an Array?

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Syntax
# MAGIC
# MAGIC      array(columns)
# MAGIC      columns: It represents the list of columns to be grouped together.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col, array, array_contains, size, element_at
from pyspark.sql.types import IntegerType, StringType, ArrayType, StructType, StructField

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 01**

# COMMAND ----------

data = [("Naresh", ["databricks", "Azure", "SQL"], [5, 7, 8]),
        ("Kalmesh", ["ADF", "AWS", "Spark", "SQL"], [6, 2, 3, 12]),
        ("Rohit", ["GCC", "PySpark", "Devops"], [3, 6, 8]),
        ("Kumar", ["GitHub", "PowerBi", "Tableau", "SQL"], [2, 5, 6, 10]),
        ("Rohini", ["SQL Editor", "Python", "Oracle", "Azure", "AWS"], [4, 7, 3, 8, 9])]

columns = ["Name", "Technology", "Experience"]

df = spark.createDataFrame(data=data, schema=columns)
display(df)

# COMMAND ----------

schema = StructType([StructField("Name", StringType(), True),
                     StructField("Technology", ArrayType(StringType())),
                     StructField("Experience", ArrayType(IntegerType()))]
                    )

# COMMAND ----------

df1 = spark.createDataFrame(data=data, schema=schema)
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) How to transform Integer Column to Array Type?

# COMMAND ----------

df_ex1 = spark.createDataFrame([("Aman", "Chennai", 22), ("Sundar", "Bangalore", 25), ("Sheela", "Hyderabad", 30), ("Shobha", "Noida", 35), ("Behra", "Mumbai", 36)], ("name", "City", "age"))
display(df_ex1)

# COMMAND ----------

# MAGIC %md
# MAGIC      # 12.2 LTS (includes Apache Spark 3.3.2, Scala 2.12)
# MAGIC      from pyspark.sql.functions import size
# MAGIC
# MAGIC      # 15.4 LTS (includes Apache Spark 3.5.0, Scala 2.12)
# MAGIC      from pyspark.sql.functions import array_size

# COMMAND ----------

# MAGIC %md
# MAGIC      from pyspark.sql.functions import size
# MAGIC      df.withColumn("Size", size("Array"))
# MAGIC                     (or)
# MAGIC      import pyspark.sql.functions as F
# MAGIC      df.withColumn("Size", F.size("Array"))

# COMMAND ----------

df_ex = df_ex1.select("age", array('age', 'age').alias("Array"))\
              .withColumn("Size", F.size("Array"))
display(df_ex)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) How to transform String Columns to Array Type?

# COMMAND ----------

# DBTITLE 1,create dataframe
data = [("Adarsh", "Chennai", "Cochin", "Hyderabad"),
        ("Akash", "Coimbatore", "Mumbai", "Chennai"),
        ("Senthil", "Salem", "Bangalore", None),
        ("Kalyan", "Delhi", "Bangalore", "Noida"),
        ("Sohile", "Mumbai", "Pune", "Cochin"),
        ("Gouthami", "Chennai", "Mumbai", None),
        ("Hemanth", "Delhi", "Noida", "Kolkata")
        ]
 
columns = ["Name", "pLocation", "sLocation", "aLocation"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### a) How to create an ArrayType column from existing columns (using SELECT)

# COMMAND ----------

# MAGIC %md
# MAGIC      # Method 1:
# MAGIC      df.select("name", array("pLocation", "sLocation", "aLocation").alias("Pref_Loc")).show(truncate=False)
# MAGIC  
# MAGIC      # Method 2:
# MAGIC      pref_Loc = ["pLocation", "sLocation", "aLocation"]
# MAGIC      df.select("name", array(*prefs_col).alias("Pref_Loc")).show(truncate=False)
# MAGIC  
# MAGIC      # Method 3:
# MAGIC      pref_Loc = ["pLocation", "sLocation", "aLocation"]
# MAGIC      df.select("name", array([col(prefLoc) for prefLoc in pref_col]).alias("Pref_Loc")).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,select: Method 01
# Method 1:
df_S_M1 = df.select("name", array("pLocation", "sLocation", "aLocation").alias("Pref_Loc"))\
            .withColumn("Size", F.size("Pref_Loc"))
df_S_M1.printSchema()
display(df_S_M1)

# COMMAND ----------

# DBTITLE 1,select: Method 02
# Method 2:
pref_Loc = ["pLocation", "sLocation", "aLocation"]
df_S_M2 = df.select("name", array(*pref_Loc).alias("Pref_Loc"))\
            .withColumn("Size", F.size("Pref_Loc"))
df_S_M2.printSchema()
display(df_S_M2)

# COMMAND ----------

# DBTITLE 1,select: Method 03
# Method 3:
pref_Loc = ["pLocation", "sLocation", "aLocation"]
df_S_M3 = df.select("name", array([col(prefLoc) for prefLoc in pref_Loc]).alias("Pref_Loc"))\
            .withColumn("Size", F.size("Pref_Loc"))
df_S_M3.printSchema()
display(df_S_M3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) How to create an ArrayType column from existing columns (using withColumn)

# COMMAND ----------

# MAGIC %md
# MAGIC      # Method 1:
# MAGIC      df.withColumn("preferences", array("pLocation", "sLocation", "aLocation")).select("name", "preferences").show(truncate=False)
# MAGIC      
# MAGIC      # Method 2:
# MAGIC      pref_Loc = ["pLocation", "sLocation", "aLocation"]
# MAGIC      df.withColumn("preferences", array(*pref_Loc)).select("name", "preferences").show(truncate=False)
# MAGIC
# MAGIC      # Method 3:
# MAGIC      pref_Loc = ["pLocation", "sLocation", "aLocation"]
# MAGIC      df.withColumn("preferences", array([col(prefLoc) for prefLoc in pref_Loc])).select("name", "preferences").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,withColumn: Method 01
# Method 1:
df_wC_M1 = df.withColumn("preferences", array("pLocation", "sLocation", "aLocation"))\
             .withColumn("Size", F.size("preferences"))\
             .select("name", "preferences", "Size")
df_wC_M1.printSchema()
display(df_wC_M1)

# COMMAND ----------

# DBTITLE 1,withColumn: Method 02
# Method 2:
pref_Loc = ["pLocation", "sLocation", "aLocation"]
df_wC_M2 = df.withColumn("preferences", array(*pref_Loc))\
             .withColumn("Size", F.size("preferences"))\
             .select("name", "preferences", "Size")
df_wC_M2.printSchema()
display(df_wC_M2)

# COMMAND ----------

# DBTITLE 1,withColumn: Method 03
# Method 3:
pref_Loc = ["pLocation", "sLocation", "aLocation"]
df_wC_M3 = df.withColumn("preferences", array([col(prefLoc) for prefLoc in pref_Loc]))\
             .withColumn("Size", F.size("preferences"))\
             .select("name", "preferences", "Size")
df_wC_M3.printSchema()
display(df_wC_M3)