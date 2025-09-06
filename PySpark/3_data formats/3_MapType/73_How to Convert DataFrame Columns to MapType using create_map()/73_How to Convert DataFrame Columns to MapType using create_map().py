# Databricks notebook source
# MAGIC %md
# MAGIC #### **How to Convert DataFrame Columns to MapType**
# MAGIC
# MAGIC - To convert DataFrame columns to a **MapType (dictionary)** column in PySpark, you can use the **create_map** function from the pyspark.sql.functions module.
# MAGIC - This function allows you to create a **map** from a set of **key-value pairs**, where the **keys and values** are columns from the DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax**
# MAGIC
# MAGIC      create_map(*columns)
# MAGIC - It represents the **column names** or Columns that are **grouped** as **key-value pairs**.   

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, create_map, concat

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Sample dataframe**

# COMMAND ----------

# DBTITLE 1,create sample dataframe
data = [("Vikas", "36636", "IT", 300000, "USA", 25), 
        ("Mukul" ,"40288", "Finance", 500000, "IND", 35), 
        ("Kannan" ,"42114", "Sales", 390000, "AUS", 23), 
        ("Rishab" ,"39192", "Marketing", 250000, "CAN", 31), 
        ("Amaresh" ,"34534", "Maintenance", 650000, "NZ", 37),
        ("Prakash" ,"69114", "Sales", 390000, "AUS", 23), 
        ("Pramod" ,"78192", "Marketing", 250000, "CAN", 31), 
        ("Prasad" ,"56534", "Maintenance", 650000, "NZ", 37),
        ]

schema = StructType([
     StructField('Name', StringType(), True),
     StructField('ProductId', StringType(), True),
     StructField('deptartment', StringType(), True),
     StructField('budget', IntegerType(), True),
     StructField('location', StringType(), True),
     StructField('Age', IntegerType(), True)
     ])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **How to create a MapType column in PySpark Azure Databricks using various methods**
# MAGIC - All below codes generates the same output.
# MAGIC
# MAGIC       from pyspark.sql.functions import create_map, col
# MAGIC
# MAGIC       # Method 1:
# MAGIC       df.select("*", create_map("name", "age")).show()
# MAGIC
# MAGIC       # Method 2:
# MAGIC       df.select("*", create_map(["name", "age"])).show()
# MAGIC
# MAGIC       # Method 3:
# MAGIC       df.select("*", create_map(col("name"), col("age"))).show()
# MAGIC
# MAGIC       # Method 4:
# MAGIC       df.select("*", create_map([col("name"), col("age")])).show()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Convert Columns to dictionary**

# COMMAND ----------

df_dict = df.select('*', create_map(col('deptartment'), col('budget')).alias('dept_sal'))
display(df_dict)

# COMMAND ----------

df_dict.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **AVRO Schema for Dept_Metadata**
# MAGIC
# MAGIC      {
# MAGIC        "name": "Dept_Metadata",
# MAGIC        "type": [
# MAGIC          "null",
# MAGIC          {
# MAGIC            "type": "map",
# MAGIC            "values": "string"
# MAGIC          }
# MAGIC        ],
# MAGIC        "doc": "key value pair, e.g deptartment, budget",
# MAGIC        "default": null
# MAGIC      }

# COMMAND ----------

# Convert columns to Map
df_map = df.withColumn("Dept_Metadata", create_map(lit("deptartment"), col("deptartment"),
                                                   lit("budget"), col("budget"))
                       )

df_map.printSchema()
display(df_map)

# COMMAND ----------

df_map.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **AVRO Schema for Dept_Metadata**
# MAGIC
# MAGIC      {
# MAGIC        "name": "Dept_Metadata",
# MAGIC        "type": [
# MAGIC          "null",
# MAGIC          {
# MAGIC            "type": "map",
# MAGIC            "values": "string"
# MAGIC          }
# MAGIC        ],
# MAGIC        "doc": "key value pair, e.g Name, deptartment, budget, location",
# MAGIC        "default": null
# MAGIC      }

# COMMAND ----------

# Convert columns to Map
df_map1 = df.withColumn("propertiesMap", create_map(lit("Name"), col("Name"),
                                                    lit("deptartment"), col("deptartment"),
                                                    lit("budget"), col("budget"),
                                                    lit("location"), col("location")))\
            .drop('Name', 'deptartment', 'budget', 'location')
df_map1.printSchema()
display(df_map1)

# COMMAND ----------

df_map1.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,concat with lit
# Convert columns to Map
df_map2 = df.withColumn("propertiesMap", create_map(lit("Name"), concat(lit("Mr."), col("Name")),
                                                    lit("deptartment"), col("deptartment"),
                                                    lit("budget"), concat(lit("Rs"), col("budget"), lit("/-")),
                                                    lit("location"), col("location")))\
            .drop('Name', 'deptartment', 'budget', 'location')
df_map2.printSchema()
display(df_map2)

# COMMAND ----------

df_map2.show(truncate=False)
