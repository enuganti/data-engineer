# Databricks notebook source
# MAGIC %md
# MAGIC #### **to_json**
# MAGIC
# MAGIC - **Converts** a column containing a **StructType, ArrayType or a MapType** into a **JSON string**.
# MAGIC
# MAGIC - Throws an **exception**, in the case of an **unsupported type**.

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

from pyspark.sql.functions import lit, col, to_json
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, StructType, StructField

# COMMAND ----------

data = [('Rajesh', ('BMW', 'ADF', 'Data Engineer', 5)),
        ('Rajasekar', ('HONDA', 'ADB', 'Developer', 8)),
        ('Harish', ('MARUTI', 'AZURE', 'Testing', 9)),
        ('Kamalesh', ('BENZ', 'PYSPARK', 'Developer', 10)),
        ('Jagadish', ('FORD', 'PYTHON', 'ADE', 3)),
        ('Arijit', ('KIA', 'DEVOPS', 'CI/CD', 4))]

schema_sub = StructType([StructField('make', StringType()), StructField('Technology', StringType()),\
                         StructField('Designation', StringType()), StructField('Experience', StringType())])
schema = StructType([StructField('Name', StringType()), StructField('Description', schema_sub)])

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()

# COMMAND ----------

df_str = df.withColumn("msg_Description", to_json("Description"))
display(df_str)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **How to convert StructType column into StringType using to_json?**

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/to_json.csv", header=True, inferSchema=True)
display(df.limit(10))

# COMMAND ----------

# Select all columns and structure them into a single column named 'pp_msg'
df_final = df.select(f.struct('*').alias('pp_msg')).distinct()
df_final.display()

# COMMAND ----------

# Convert the 'pp_msg' column to JSON string
df_final = df_final.withColumn('pp_msg_json', f.to_json(f.col('pp_msg')))
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reference**
# MAGIC
# MAGIC     - https://stackoverflow.com/questions/49602965/pyspark-dataframe-to-json-function
