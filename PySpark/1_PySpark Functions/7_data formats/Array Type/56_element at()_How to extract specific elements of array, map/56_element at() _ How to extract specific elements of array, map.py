# Databricks notebook source
# MAGIC %md
# MAGIC ### element_at
# MAGIC
# MAGIC - used to **extract** specific elements from **array or map** columns within a DataFrame.
# MAGIC - Works with both **ArrayType and MapType columns**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1) element_at() on an array()
# MAGIC
# MAGIC - For **arrays**, indexing **starts at 1**;
# MAGIC   - **Positive indices** access elements from the **beginning** (e.g., **index 1** for the **first element**).
# MAGIC   - If the **index is negative**, elements are accessed from the **end of the array towards the beginning**. (e.g., **index -1** for the **last element**).
# MAGIC   - Using **index 0** will result in an **error**.
# MAGIC - If the **index is out of array boundaries**
# MAGIC   - **spark.sql.ansi.enabled** is **true**, an exception is thrown.
# MAGIC   - otherwise, **NULL** is returned.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Syntax
# MAGIC
# MAGIC      element_at(array_col, index)

# COMMAND ----------

from pyspark.sql.functions import element_at

data = [('Jay', ['Java','Scala','Python','PySpark',None], {'product':'bmw', 'color':'brown', 'type':'sedan'}),
        ('Midun', ['Spark','Java','Spark SQL','SQL',None], {'product':'audi', 'color':None, 'type':'truck'}),
        ('Robert', ['CSharp','',None,'VS Code'], {'product':'volvo', 'color':'', 'type':'sedan'}),
        ('Paul', None, None),
        ('Basha', ['1','2','3','4','5'], {}),
        ('Gopesh', ['Python','R','SQL','Tableau'], {'product':'tesla', 'color':'red', 'type':'electric'}),
        ('Bobby', ['Go','Rust','C++','Docker'], {'product':'ford', 'color':'blue', 'type':'suv'}),
        ('Chetan', ['JavaScript','React','NodeJS','MongoDB'], {'product':'toyota', 'color':'white', 'type':'hatchback'}),
        ('Dravid', ['Python','Java','AWS','Spark'], {'product':'honda', 'color':'black', 'type':'sedan'}),
        ('Eshwar', ['C','C++','Embedded','Matlab'], {'product':'nissan', 'color':'grey', 'type':'truck'}),
        ('Firoz', ['Scala','Spark','Kafka'], {'product':'mercedes', 'color':'silver', 'type':'suv'}),
        ('Gayathri', ['Java','Spring Boot','Hibernate'], {'product':'bmw', 'color':'blue', 'type':'coupe'}),
        ('Hemanth', ['Python','Pandas','Numpy','ML'], {'product':'audi', 'color':'green', 'type':'sedan'}),
        ('Ishan', ['Ruby','Rails','Postgres'], {'product':'volkswagen', 'color':'yellow', 'type':'hatchback'}),
        ('Jack', ['Kotlin','Android','Firebase'], {'product':'hyundai', 'color':'white', 'type':'suv'})
       ]

columns = ['name','knownLanguages','properties']

df_array = spark.createDataFrame(data, columns)
display(df_array)

# COMMAND ----------

df_array.select(
    "name",
    element_at("knownLanguages", 1).alias("first_element"),
    element_at("knownLanguages", 2).alias("second_element"),
    element_at("knownLanguages", 3).alias("third_element"),
    element_at("knownLanguages", -1).alias("last_element")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get Element At
# MAGIC - **element_at()** also works with **dynamic indexes or keys** passed as column **expressions or literal values**.
# MAGIC - For example, you can use **lit(-1)** to **dynamically retrieve** the **last element** of an **array**.

# COMMAND ----------

# Retrieve the last element of an array using lit() 
from pyspark.sql.functions import lit

df_array.select("knownLanguages",
                element_at("knownLanguages", lit(1)).alias("first_element"),
                element_at("knownLanguages", lit(2)).alias("second_element"),
                element_at("knownLanguages", lit(-1)).alias("last_element")
                ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) element_at() on an map
# MAGIC
# MAGIC - It returns the value associated with a given key.
# MAGIC - If the **key** is **not found** in the **map**, it returns **NULL**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Syntax
# MAGIC
# MAGIC      element_at(map_col, key)
# MAGIC - Returns the **value** for the given key.

# COMMAND ----------

# DBTITLE 1,Method 01
df_array.select(
    "properties",
    element_at("properties", "product").alias("Product"),
    element_at("properties", "color").alias("Color"),
    element_at("properties", "type").alias("Type")
).display()

# COMMAND ----------

# DBTITLE 1,Method 02
df_array.select(
    "properties",
    element_at("properties", lit("product")).alias("Product"),
    element_at("properties", lit("color")).alias("Color"),
    element_at("properties", lit("type")).alias("Type"),
    element_at("properties", lit("height")).alias("height")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Get a Non-Existing Value from a Map using a Key**
# MAGIC - If you want to retrieve a value for a key that **doesn’t exist** in the **map**, element_at() function will return **NULL** instead of throwing an **error**.

# COMMAND ----------

# Get a Non-Existing Value from a Map using a Key
df_array.select(
    "properties",
    element_at("properties", "height").alias("Non_Existing")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) element_at() in Array and Map Together

# COMMAND ----------

# Implement both array and map columns uisng element_at()
df_array.select("knownLanguages", "properties",
                element_at("knownLanguages", 1).alias("first_element"),
                element_at("knownLanguages", -1).alias("last_element"),
                element_at("properties", "product").alias("Product"),
                element_at("properties", "color").alias("Color")
                ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4) Array of Maps

# COMMAND ----------

data = [
    (1, [{"k": "a", "v": "100"}, {"k": "b", "v": "200"}]),
    (2, [{"k": "x", "v": "111"}, {"k": "y", "v": "222"}]),
    (3, [{"k": "c", "v": "120"}, {"k": "d", "v": "300"}]),
    (4, [{"k": "e", "v": "121"}, {"k": "f", "v": "332"}]),
    (5, [{"k": "g", "v": "130"}, {"k": "h", "v": "400"}]),
    (6, [{"k": "i", "v": "161"}, {"k": "j", "v": "442"}]),
    (7, [{"k": "k", "v": "150"}, {"k": "l", "v": "500"}]),
    (8, [{"k": "m", "v": "191"}, {"k": "n", "v": "662"}])
]

df_arr_map = spark.createDataFrame(data, ["id", "items"])
display(df_arr_map)

# COMMAND ----------

df_arr_map.select(
    "items",
    element_at("items", 1).alias("first_item"),
    element_at("items", -1).alias("last_item")
).display()