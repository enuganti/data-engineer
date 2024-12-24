# Databricks notebook source
# MAGIC %md
# MAGIC **Q) How to Explode nested array into rows?**

# COMMAND ----------

from pyspark.sql.functions import col, explode, posexplode, flatten

data = [("Revanth", [["ADF", "Spark", "ADB"], ["ETL", "Devops", None], ["SQL", None]]),
        ("Reshma", [["SSMS", None, "Salesforce"], ["SAP", "ERP", None]]),
        ("Raashi", [["Python" "VB", None], ["C++", "GitHub", "Git"]]),
        ("Krishna", [["SHELL", "DRG"], ["JAVA", None]]),
        ("Sudarshan", None),
        ("Kamal", [])
       ]

columns = ["EmpName", "Technology"]

df = spark.createDataFrame(data=data, schema=columns)
display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**: Flatten | Explode

# COMMAND ----------

# MAGIC %md
# MAGIC      df1 = df.select(col("EmpName"), posexplode(col("Technology")).alias('pos', 'CoreTechnology'))\
# MAGIC              .withColumnRenamed("pos", "Index")
# MAGIC                         (or)
# MAGIC      df1 = df.select(col("EmpName"), posexplode(col("Technology")))\
# MAGIC              .withColumnRenamed("col", "CoreTechnology")\
# MAGIC              .withColumnRenamed("pos", "Index")                    

# COMMAND ----------

df1 = df.select(col("EmpName"), posexplode(col("Technology")).alias('pos', 'CoreTechnology'))\
        .withColumnRenamed("pos", "Index")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC - but we would end up with this dataframe, which we have **1 row per array**. That's not exactly what we want... we are trying to obtain **1 row per element for each one of the names**.

# COMMAND ----------

df2 = df.select(col("EmpName"), flatten(col("Technology")).alias('CoreTechnology'))
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC - It will generate a dataframe like this one bellow. Now we have **all the elements, in a singles array per row**.

# COMMAND ----------

# MAGIC %md
# MAGIC      df3 = df2.select(col("EmpName"), posexplode(col("CoreTechnology")))\
# MAGIC               .withColumnRenamed("col", "FlattenTechnology")\
# MAGIC               .withColumnRenamed("pos", "Index")
# MAGIC                              (or)
# MAGIC      df3 = df2.select(col("EmpName"), posexplode(col("CoreTechnology")).alias('pos', 'FlattenTechnology'))\
# MAGIC               .withColumnRenamed("pos", "Index")
# MAGIC                              (or)
# MAGIC      df3 = df2.withColumn("CoreTechnology", explode(col("CoreTechnology")).alias('FlattenTechnology'))

# COMMAND ----------

df3 = df2.select(col("EmpName"), posexplode(col("CoreTechnology")))\
         .withColumnRenamed("col", "FlattenTechnology")\
         .withColumnRenamed("pos", "Index")
display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 02**: Explode | Explode

# COMMAND ----------

# MAGIC %md
# MAGIC #### **a) SELECT**

# COMMAND ----------

dff = df.select("*", explode(col("Technology"))).withColumnRenamed("col", "New_Tech")
display(dff)

# COMMAND ----------

dff = dff.select("*", explode(col("New_Tech"))).drop("Technology", "New_Tech")\
         .withColumnRenamed("col", "Final_Tech")
display(dff)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **b) withColumn**

# COMMAND ----------

# First, explode the outer array
df_outer_exploded = df.withColumn("inner_array", explode(col("Technology")))
display(df_outer_exploded)

# COMMAND ----------

# Then, explode the inner array
df_inner_exploded = df_outer_exploded.withColumn("FlattenTechnology", explode(col("inner_array")))
display(df_inner_exploded)

# COMMAND ----------

# Drop the intermediate column if not needed
df_final = df_inner_exploded.drop("inner_array")
display(df_final)
