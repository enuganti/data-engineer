# Databricks notebook source
# MAGIC %md
# MAGIC #### **array_contains**
# MAGIC
# MAGIC - used to check if **array column contains a specific value**.
# MAGIC - It is commonly used in **filtering** operations.
# MAGIC - It returns a **Boolean column** indicating the **presence** of the element in the array.
# MAGIC   - **True**: If the value is **present**.
# MAGIC   - **False**: If the value is **not present**.
# MAGIC   - **null**: If the array column is **null/None**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax**
# MAGIC
# MAGIC      array_contains(array_column, value)
# MAGIC      
# MAGIC **column (str, Column):** It represents a column of ArrayType
# MAGIC
# MAGIC **value (str):** It represents the value to check if it is in the array column
# MAGIC
# MAGIC **Returns**: BOOLEAN

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT array_contains(array(1, 2, 3), 2) AS Boolean;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT array_contains(array(1, NULL, 3), 2) AS Boolean;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT array_contains(array(1, 4, 3), 2) AS Boolean;

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from pyspark.sql.functions import array_contains

# COMMAND ----------

data = [("Anand", ["Java","Scala","C++"], ["Spark","Java","Azure Databricks"], [8, 9, 5, 7]),
        ("Berne", ["Python","PySpark","C"], ["spark sql","ADF","SQL"], [11, 3, 6, 8]),
        ("Charan", ["Devops","VB","Git"], ["ApacheSpark","Python"], [5, 6, 8, 10]),
        ("Denish", ["SQL","Azure","AWS"], ["PySpark","Oracle","Confluence"], [12, 6, 8, 15]),
        ("Krishna", ["GCC","Visual Studio","Python"], ["SQL","Databricks","SQL Editor"], [2, 6, 5, 8]),
        ("Hari", ["Devops","VB","Git"], ["ApacheSpark","Python"], [5, 6, 8, 10]),
        ("Rakesh", ["SQL","Azure","AWS"], ["PySpark","Oracle","SQL"], [12, 6, 8, 15]),
        ("karan", ["AWS","Visual Studio","Python"], ["SQL","Git","SQL Editor"], [2, 6, 5, 8]),
        ("Eren", None, None, None)]
 
columns = ["Full_Name", "Languages", "New_Languages", "Experience"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) How to check value is present in a column?**
# MAGIC - Function **checks** if the specified **value is present** in an **array column or not**.

# COMMAND ----------

# to find out whether the students know Python or not.
df_con_py = df.select("Full_Name", "Languages", array_contains("Languages", "Python").alias("knowns_python"))
display(df_con_py)

# COMMAND ----------

#  to find out whether the students know Java or not.
df_con_ja = df.withColumn("knowns_java", array_contains("New_Languages", "SQL"))\
              .select("Full_Name", "New_Languages", "knowns_java")
display(df_con_ja)

# COMMAND ----------

df_arr_con = df.select("Full_Name",\
                       "Languages", array_contains(df.Languages, "SQL").alias("Knows_Python"),\
                       "New_Languages", array_contains(df.New_Languages, "PySpark").alias("Knows_PySpark"),\
                       "Experience", array_contains(df.Experience, 8).alias("Experience"))
display(df_arr_con)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) How to filter records using array_contains()?**
# MAGIC
# MAGIC - To **filter** out students **who know “Python”** using array_contains() as a condition.

# COMMAND ----------

df_con_py_filt = df.select("Full_Name", "Languages") \
                   .filter(array_contains("Languages", "Python"))
display(df_con_py_filt)

# COMMAND ----------

df_con_py_filt1 = df.select("Full_Name", "New_Languages") \
                    .filter(array_contains("New_Languages", "SQL"))
display(df_con_py_filt1)+
