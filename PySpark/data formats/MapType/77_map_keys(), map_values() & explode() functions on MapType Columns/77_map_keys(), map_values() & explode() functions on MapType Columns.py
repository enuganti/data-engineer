# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType, MapType

schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(), StringType()), True)
])

# COMMAND ----------

Data_Dictionary = [
        ('Ram',{'hair':'brown','eye':'blue'}),
        ('Shyam',{'hair':'black','eye':'black'}),
        ('Amit',{'hair':'grey','eye':None}),
        ('Aupam',{'hair':'red','eye':'black'}),
        ('Rahul',{'hair':'black','eye':'grey'})
        ]

df_map = spark.createDataFrame(data = Data_Dictionary, schema = schema)
df_map.printSchema()
display(df_map)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **map_keys()**
# MAGIC
# MAGIC - Getting **keys and values** using **map_key** function

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      map_values(*column)

# COMMAND ----------

from pyspark.sql.functions import map_keys

# COMMAND ----------

# To extract keys we can use map_key() 
display(df_map.select(df_map.name, map_keys(df_map.properties).alias('map_values')))

# COMMAND ----------

df_map.select(df_map.name, map_keys(df_map.properties).alias('map_values')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **map_values()**
# MAGIC - Getting **keys and values** using **map_values**.

# COMMAND ----------

from pyspark.sql.functions import map_values

display(df_map.select(df_map.name, map_values(df_map.properties).alias('map_values')))

# COMMAND ----------

df_map.select(df_map.name, map_values(df_map.properties).alias('map_values')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **explode**
# MAGIC
# MAGIC - Getting the **keys and values** using **explode** function.

# COMMAND ----------

from pyspark.sql.functions import explode

display(df_map.select(df_map.name, explode(df_map.properties)))

# COMMAND ----------

# MAGIC %md
# MAGIC - Getting **all the keys** MapType using **Explode** function.

# COMMAND ----------

keysDF = df_map.select(explode(map_keys(df_map.properties))).distinct()
keysList = keysDF.rdd.map(lambda x:x[0]).collect()
print(keysList)

# COMMAND ----------

# MAGIC %md
# MAGIC **How to get unique values from a MapType column**

# COMMAND ----------

# Unique keys
unique_values_df = df_map.select(df_map.name, explode(df_map.properties).alias("key", "value")).distinct()\
                         .filter("value IS NOT NULL")
display(unique_values_df)

# COMMAND ----------

# Collecting all the numeric value out of all values
unique_values_list = [record.value for record in unique_values_df.rdd.collect() if record.value.isnumeric()]
print(unique_values_list)