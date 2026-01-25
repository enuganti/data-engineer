# Databricks notebook source
# MAGIC %md
# MAGIC ##### locate()
# MAGIC
# MAGIC - The **locate()** function in PySpark is used to find the **position of a substring** within a **string**.
# MAGIC
# MAGIC - It works just like SQL's **INSTR() or POSITION()** functions.
# MAGIC
# MAGIC - The position is **not zero based**, but **1 based index**. Returns **0 if substr could not be found in str**.
# MAGIC
# MAGIC - Locate the position of the **first occurrence** of substr in a string column, after position pos.
# MAGIC
# MAGIC - If **more than one occurrence** is there in a string. It will result the **position** of the **first occurrence**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Syntax
# MAGIC
# MAGIC      locate(substr, str[, pos])
# MAGIC
# MAGIC **substr:** the substring to find
# MAGIC
# MAGIC **str:** the column where you want to search
# MAGIC
# MAGIC **pos (optional):** the position to start searching from (1-based index)

# COMMAND ----------

from pyspark.sql.functions import substring, concat, lit, col, expr, locate

# COMMAND ----------

# DBTITLE 1,sample dataset
data = [(1, "Weekly", "Repeat=yearly;term=1;endtime=20240131T000000Z;pos=1;day=TH;month=2;"),
        (2, "Assurance test", "Repeat=yearly;term=1;monthday=12;month=1;endtime=20230112T000000Z;"),
        (3, "MSC Audit", "Repeat=yearly;term=2;COUNT=10;month=8;monthday=19;"),
        (4, "Regression Test", "Repeat=monthly;term=2;COUNT=2;monthday=9;"),
        (5, "Lab Safety", "Repeat=monthly;term=3;COUNT=10;"),
        (6, "Testing", "Repeat=monthly;term=6;"),
        (7, "test", "Repeat=monthly;term=1;")]

schema = ["Id", "Type", "Pattern"]

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()

# COMMAND ----------

df1 = df.withColumn("loc", locate(";", col("Pattern")))
display(df1)

# COMMAND ----------

df_01 = df.withColumn("loc1", locate(";", col("Pattern"), 1)) \
          .withColumn("Repeat", expr("substring(Pattern, 0, (loc1-1))")) \
          .withColumn("Frequency", expr("substring(Pattern, loc1+1, length(Pattern))"))

df_01_Repeat = df_01.select("Id", "Type", "Pattern", "loc1", "Repeat", "Frequency")
display(df_01_Repeat)
df_01_Repeat.printSchema()

# COMMAND ----------

df_02 = df_01.withColumn("loc2", locate(";", col("Frequency"), 1)) \
             .withColumn("term", expr("substring(Frequency, 0, (loc2-1))")) \
             .withColumn("Frequency2", expr("substring(Frequency, loc2+1, length(Frequency))"))

df_02_term = df_02.select("Id", "Type", "Frequency", "Repeat", "loc2", "term", "Frequency2")
display(df_02_term)
df_02_term.printSchema()

# COMMAND ----------

df_03 = df_02.withColumn("loc3", locate(";", col("Frequency2"), 1)) \
             .withColumn("count", expr("substring(Frequency2, 0, (loc3-1))")) \
             .withColumn("Frequency3", expr("substring(Frequency2, loc3+1, length(Frequency2))"))

df_03_term = df_03.select("Id", "Type", "Frequency2", "Repeat", "term", "loc3", "count", "Frequency3")
display(df_03_term)
df_03_term.printSchema()

# COMMAND ----------

df_04 = df_03.withColumn("loc4", locate(";", col("Frequency3"), 1)) \
             .withColumn("endtime", expr("substring(Frequency3, 0, (loc4-1))")) \
             .withColumn("Frequency4", expr("substring(Frequency3, loc4+1, length(Frequency3))"))

df_04_term = df_04.select("Id", "Type", "Frequency3", "Repeat", "term", "count", "loc4", "endtime", "Frequency4")
display(df_04_term)
df_04_term.printSchema()

# COMMAND ----------

df_05 = df_04.withColumn("loc5", locate(";", col("Frequency4"), 1)) \
             .withColumn("day", expr("substring(Frequency4, 0, (loc5-1))")) \
             .withColumn("month", expr("substring(Frequency4, loc5+1, length(Frequency4))"))

df_05_term = df_05.select("Id", "Type", "Frequency4", "Repeat", "term", "count", "endtime", "loc5", "day", "month")
display(df_05_term)
df_05_term.printSchema()

# COMMAND ----------

df_final = df_05_term.select("Id", "Type", "Repeat", "term", "count", "endtime", "day", "month")
display(df_final)