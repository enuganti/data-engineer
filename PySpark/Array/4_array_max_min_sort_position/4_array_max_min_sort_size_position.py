# Databricks notebook source
# MAGIC %md
# MAGIC #### **Array Functions**
# MAGIC
# MAGIC - array_max
# MAGIC - array_min
# MAGIC - array_sort
# MAGIC - array_position
# MAGIC - array_size

# COMMAND ----------

# DBTITLE 1,Load Required Libraries
from pyspark.sql.types import IntegerType, StringType, ArrayType, StructType, StructField
from pyspark.sql.functions import explode, split, array, array_contains, array_max, array_min, array_sort, array_position, array_size, element_at

# COMMAND ----------

# DBTITLE 1,Create Dataframe
data = [("Akash", ["Java", "Scala", "C++"], ["Spark", "Java", "Azure Databricks"], [8, 9, 8, 5, 7]),
        ("Ramprasad", ["Python", "PySpark", "C", "Python"], ["spark sql", "ADF"], [11, 8, 3, 6, 8]),
        ("Rohit", ["Scala", "Devops", "VB", "Git"], ["ApacheSpark", "Python"], [5, 6, 8, 10, 8]),
        ("Raju", ["SQL", "Azure", "Scala", "AWS"], ["PySpark", "Oracle", "Confluence"], [12, 6, 5, 8, 8]),
        ("Kamalakar", ["GCC", "Visual Studio", "Python"], ["SQL", "Databricks", "SQL"], [1, 2, 6, 5, 8]),
        ("Swetha", ["Devops", "VB", "Git", "Scala"], ["ApacheSpark", "Python"], [5, 6, 8, 10, 3]),
        ("Mallik", ["SQL", "Azure", "AWS"], ["PySpark", "Oracle", "Schema"], [12, None, 8, 15, 9]),
        ("Deepak", ["AWS", "Python", "Scala"], ["Git", "SQL Editor", "SQL"], [2, 6, 5, 8, 4]),
        ("Kiran", ["Scala", "Python", "Spark"], ["ADF", "AWS", "SQL"], [None, None, None, None, None]),
        ("Sumanth", ["Python", "Scala", "ADF"], ["KSql", "Databricks", "SQL"], [])
       ]

schema = StructType([
    StructField("FullName", StringType(), True),
    StructField("LearntLanguages", ArrayType(StringType()), True),
    StructField("ToLearnLanguages", ArrayType(StringType()), True),
    StructField("Rating", ArrayType(IntegerType()), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) array_max**
# MAGIC
# MAGIC - returns the **maximum value** of the array.
# MAGIC - **NULL** elements are **skipped**.
# MAGIC - If array is **empty**, or contains **only NULL** elements, NULL is returned.
# MAGIC - array_max works with **arrays of numeric types**. It finds the **maximum** value in an array of **numeric elements**.
# MAGIC - If you try to use it on an **array of strings**, it will **not work as intended** because it's not designed to compare string values or determine the "maximum" string in terms of **alphabetical or lexicographical order**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax**
# MAGIC
# MAGIC      array_max(array)
# MAGIC      array: Any ARRAY with elements for which order is supported

# COMMAND ----------

# DBTITLE 1,array_max()
arr_max = df.withColumn("max_Rating", array_max("Rating"))\
            .select("Rating", "max_Rating")            
display(arr_max)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) array_min**
# MAGIC
# MAGIC - returns the **minimum value** of the array.

# COMMAND ----------

# DBTITLE 1,array_min()
arr_min = df.withColumn("min_Rating", array_min("Rating"))\
            .select("Rating", "min_Rating")            
display(arr_min)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) array_sort()**
# MAGIC
# MAGIC - arranges the input array in **ascending order**.
# MAGIC - When you have **NaN** values in an array, the following applies.
# MAGIC   - For **double/float type**, NaN is considered greater than any **non-NaN elements**.
# MAGIC   - **Null** elements are positioned at the **end** of the **resulting array**.

# COMMAND ----------

# DBTITLE 1,array_sort()
df_arr_sort = df.withColumn("arr_sort_Learnt", array_sort("LearntLanguages"))\
                .withColumn("arr_sort_ToLearn", array_sort("ToLearnLanguages"))\
                .withColumn("arr_sort_Rating", array_sort("Rating"))\
                .select("LearntLanguages", "arr_sort_Learnt", "ToLearnLanguages", "arr_sort_ToLearn", "Rating", "arr_sort_Rating")
display(df_arr_sort)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) array_size()**
# MAGIC
# MAGIC - The array_size() returns the total **number of elements** in the **array column**.
# MAGIC - If your input array column is **null**, it returns **null**.

# COMMAND ----------

# array_size is not available in 12.2 LTS (includes Apache Spark 3.3.2, Scala 2.12). instead use size.
from pyspark.sql.functions import size

# array_size is available in 15.4 LTS (includes Apache Spark 3.5.0, Scala 2.12).
from pyspark.sql.functions import array_size

# COMMAND ----------

# DBTITLE 1,array_size()
df_arr_size = df.withColumn("Arr_Size_Learnt", array_size("LearntLanguages"))\
                .withColumn("Arr_Size_ToLearn", array_size("ToLearnLanguages"))\
                .withColumn("Arr_Size_Rating", array_size("Rating"))\
                .select("LearntLanguages", "Arr_Size_Learnt", "ToLearnLanguages", "Arr_Size_ToLearn", "Rating", "Arr_Size_Rating")
display(df_arr_size)

# COMMAND ----------

# DBTITLE 1,filter based on array size: LearntLanguages
df_arr_Lernt_filt = df.select("LearntLanguages")\
                      .filter(array_size("LearntLanguages") > 3)                     
display(df_arr_Lernt_filt)

# COMMAND ----------

# DBTITLE 1,filter based on array size: ToLearnLanguages
df_arr_ToLernt_filt = df.select("ToLearnLanguages")\
                        .filter(array_size("ToLearnLanguages") > 2)
display(df_arr_ToLernt_filt)

# COMMAND ----------

# DBTITLE 1,filter based on array size: Rating
df_arr_Rat_filt = df.select("Rating")\
                    .filter(array_size("Rating") > 2)
display(df_arr_Rat_filt)

# COMMAND ----------

# DBTITLE 1,multiple filter conditions
df_arr_size_filt = df.select("LearntLanguages", "ToLearnLanguages", "Rating")\
                     .filter(array_size("LearntLanguages") > 3)\
                     .filter(array_size("ToLearnLanguages") > 2)\
                     .filter(array_size("Rating") > 2)
display(df_arr_size_filt)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **5) array_position()**
# MAGIC
# MAGIC - To find the **position** of the **first occurrence** of the value in the given **array**.
# MAGIC - It returns **null** if **either of the arguments is null**.
# MAGIC - Note that the position is **not zero-based** but **1 1-based index**.
# MAGIC - Returns **0** if the value could **not be found** in the array.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax**
# MAGIC
# MAGIC      array_position(column, value)

# COMMAND ----------

# DBTITLE 1,array_position()
df_arr_pos = df.withColumn("Learnt_position", array_position("LearntLanguages", "Python"))\
               .withColumn("ToLearn_position", array_position("ToLearnLanguages", "SQL"))\
               .withColumn("ToLearn_NULL_position", array_position("ToLearnLanguages", "Git"))\
               .withColumn("Rating_position", array_position("Rating", 8))\
               .select("LearntLanguages", "Learnt_position", "ToLearnLanguages", "ToLearn_position", "ToLearn_NULL_position", "Rating", "Rating_position")
display(df_arr_pos)

# COMMAND ----------

df_arr_ele_pos = df.withColumn("Learnt_position", element_at("LearntLanguages", 2))\
                   .withColumn("Learnt_position_Elemt", array_position("LearntLanguages", "Scala"))\
                   .select("LearntLanguages", "Learnt_position", "Learnt_position_Elemt")
display(df_arr_ele_pos) 

# COMMAND ----------

trns_data = df.withColumn("max", array_max("ToLearnLanguages"))\
              .withColumn("min", array_min("ToLearnLanguages"))\
              .withColumn("sort", array_sort("ToLearnLanguages"))\
              .withColumn("location", array_position("Rating",8))\
              .select("ToLearnLanguages", "Rating", "max", "min", "sort", "location")

display(trns_data)
