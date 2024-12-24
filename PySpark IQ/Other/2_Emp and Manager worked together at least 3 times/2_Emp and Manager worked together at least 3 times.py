# Databricks notebook source
# MAGIC %md
# MAGIC **𝐏𝐑𝐎𝐁𝐋𝐄𝐌 𝐒𝐓𝐀𝐓𝐄𝐌𝐄𝐍𝐓**
# MAGIC
# MAGIC - Write a PYSPARK program to generate a report that provides **pairs (Emp_ID, Manager_ID)** where the **Employee & Manager worked together at least 3 times**.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

schema = StructType([StructField("Emp_ID", IntegerType(), True),
                     StructField("Emp_Name", StringType(), True),
                     StructField("Manager_ID", IntegerType(), True),
                     StructField("Manager_Name", StringType(), True),
                     StructField("Emp_Age", IntegerType(), True)
                    ])

data = [(1, "Sekhar", 1, "Swaroop", 30),
        (1, "Sekhar", 1, "Swaroop", 41),
        (1, "Sekhar", 1, "Swaroop", 32),
        (1, "Sekhar", 2, "Prakash", 43),
        (1, "Sekhar", 2, "Prakash", 24),
        (2, "Stanely", 1, "Swaroop", 35),
        (2, "Stanely", 1, "Swaroop", 26),
        (3, "Dinesh", 2, "Prakash", 35),
        (3, "Dinesh", 2, "Prakash", 26),
        (3, "Dinesh", 2, "Prakash", 26)
       ]

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

df_count = df.groupBy("Emp_ID", "Emp_Name", "Manager_ID", "Manager_Name").count()
display(df_count)

# COMMAND ----------

df_count.filter(df_count['count']>=3).display()
