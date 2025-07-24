# Databricks notebook source
# MAGIC %md
# MAGIC #### **locate()**
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
# MAGIC #### **Syntax**
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
data = [(1, "Weekly", "FREQUENCY=YEARLY;INTERVAL=5;UNTIL=20980131T000000Z;BYSETPOS=6;BYDAY=TH;BYMONTH=12;"),
        (2, "Smoke test", "FREQUENCY=YEARLY;INTERVAL=7;BYMONTHDAY=12;BYMONTH=1;UNTIL=20230112T000000Z;"),
        (3, "MSC Audit", "FREQUENCY=YEARLY;INTERVAL=2;COUNT=10;BYMONTH=8;BYMONTHDAY=19;"),
        (4, "Regression Test", "FREQUENCY=MONTHLY;INTERVAL=5;COUNT=9;BYMONTHDAY=29;"),
        (5, "Random Test", "FREQUENCY=MONTHLY;INTERVAL=19;COUNT=10;"),
        (6, "Testing", "FREQUENCY=MONTHLY;INTERVAL=14;"),
        (7, "unit test", "FREQUENCY=MONTHLY;INTERVAL=13;")]

schema = ["Id", "Title", "RecurrencePattern"]

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

df1 = df.withColumn("loc", locate(";", col("RecurrencePattern")))
display(df1)

# COMMAND ----------

df_FREQUENCY = df\
    .withColumn("loc_first", locate(";", col("RecurrencePattern"), 1)) \
    .withColumn("FREQUENCY_TYPE", expr("substring(RecurrencePattern, 0, (loc_first-1))")) \
    .withColumn("FREQUENCY", expr("substring(RecurrencePattern, loc_first+1, length(RecurrencePattern)-1)"))
display(df_FREQUENCY)

# COMMAND ----------

df_INTERVAL = df_FREQUENCY\
    .withColumn("loc_second", locate(";", col("FREQUENCY"), 1)) \
    .withColumn("INTERVAL", expr("substring(FREQUENCY, 0, (loc_second-1))")) \
    .withColumn("UNTIL", expr("substring(FREQUENCY, loc_second+1, length(FREQUENCY)-1)")) \
    .select("FREQUENCY", "loc_second", "INTERVAL", "UNTIL")
display(df_INTERVAL)

# COMMAND ----------

df_COUNT = df_INTERVAL\
    .withColumn("loc_third", locate(";", col("UNTIL"), 1)) \
    .withColumn("COUNT", expr("substring(UNTIL, 0, (loc_third-1))")) \
    .withColumn("BYMONTH", expr("substring(UNTIL, loc_third+1, length(UNTIL)-1)")) \
    .select("UNTIL", "loc_third", "COUNT", "BYMONTH")
display(df_COUNT)

# COMMAND ----------

df_BYSETPOS = df_COUNT\
    .withColumn("loc_fourth", locate(";", col("BYMONTH"), 1)) \
    .withColumn("BYSETPOS", expr("substring(BYMONTH, 0, (loc_fourth-1))")) \
    .withColumn("BYDAY", expr("substring(BYMONTH, loc_fourth+1, length(BYMONTH)-1)")) \
    .select("BYMONTH", "loc_fourth", "BYSETPOS", "BYDAY")
display(df_BYSETPOS)

# COMMAND ----------

df_final = df_BYSETPOS\
    .withColumn("loc_fifth", locate(";", col("BYDAY"), 1)) \
    .withColumn("BYMONTHDAY", expr("substring(BYDAY, 0, (loc_fifth-1))")) \
    .withColumn("final_col", expr("substring(BYDAY, loc_fifth+1, length(BYDAY)-1)")) \
    .select("BYDAY", "loc_fifth", "BYMONTHDAY", "final_col")
display(df_final)