# Databricks notebook source
# MAGIC %md
# MAGIC **collect_list()**: 
# MAGIC - Returns all values from input column as **list with duplicates**.
# MAGIC - It is an aggregate function which returns an **array** that has all values with the group.
# MAGIC - It is used to create an **array type column** on dataframe by merging rows typically after **group by or window partitions**.
# MAGIC - **order of elements** inside array is **maintained**.
# MAGIC
# MAGIC **collect_set()**:
# MAGIC - Returns all values from input column as **list without duplicates**.
# MAGIC - dedupes and **eliminates the duplicates** and results in **unique values**.
# MAGIC - **order of elements** inside array is **not maintained**.

# COMMAND ----------

from pyspark.sql.functions import collect_list, collect_set, array_distinct, struct
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as f

# COMMAND ----------

data = [(1, 'James','Java', 25),
        (2, 'James','Python', 28),
        (3, 'James','Python', 35),
        (4, 'Anna','PHP', 29),
        (5, 'Anna','Javascript', None),
        (6, 'Maria','Java', 54),
        (7, 'Maria','C++', 47),
        (8, 'James','Scala', 35),
        (9, 'Anna','PHP', None),
        (10, 'Anna','HTML', 21),
        (11, 'Anna',None, 65),
        (12, 'Rakesh', None, None)
       ]

schema = StructType([StructField("id", IntegerType(), False),
                     StructField("Name", StringType(), False),
                     StructField("Languages", StringType(), True),
                     StructField("Age", IntegerType(), True)])

df = spark.createDataFrame(data=data, schema=schema)
display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Single Column: collect_list()**

# COMMAND ----------

df1 = df.select(collect_list("languages").alias("Languages_List"))
display(df1)

# COMMAND ----------

df1 = df.select(collect_list("Age").alias("Age_List"))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Single Column: collect_set()**

# COMMAND ----------

df2 = df.select(collect_set("languages").alias("Languages_Set"))
display(df2)

# COMMAND ----------

df2 = df.select(collect_set("Age").alias("Age_Set"))
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **array_distinct --> collect_list**

# COMMAND ----------

df3 = df.select(array_distinct(collect_list("languages")).alias("Languages_List"))
display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Single Column: groupBy with collect_list() & collect_set()**

# COMMAND ----------

df4 = df.groupBy("name").agg(collect_list("languages").alias("List_Languages"))
display(df4)
df4.printSchema() 

# COMMAND ----------

df5 = df.groupBy("name").agg(collect_set("languages").alias("Set_Languages"))
display(df5)
df5.printSchema() 

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Multiple Columns: groupBy --> struct --> collect_list()**

# COMMAND ----------

df.select("name").distinct().show()

# COMMAND ----------

df6 = df.groupBy("name").agg(collect_list(struct("languages", "Age")).alias("Name_Languages"))
display(df6)
df6.printSchema() 