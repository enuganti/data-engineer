# Databricks notebook source
# MAGIC %md
# MAGIC ##### sequence()
# MAGIC - The **sequence()** function in PySpark is used to **generate a sequence of values (typically dates or numbers)** between a given **start and end value**.
# MAGIC - It is often used with **arrays**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC      
# MAGIC      sequence(start, stop [, step] )
# MAGIC
# MAGIC **Parameters**
# MAGIC - **start**:  Column or str
# MAGIC   - starting value (inclusive)
# MAGIC   - An expression of an integral **numeric type, DATE, or TIMESTAMP**.
# MAGIC
# MAGIC - **stop** Column or str
# MAGIC   - last values (inclusive)
# MAGIC   - If start is numeric an integral **numeric, a DATE or TIMESTAMP otherwise**.
# MAGIC
# MAGIC - **step** Column or str, optional
# MAGIC   - value to add to current to get next element **(default is 1)**
# MAGIC   - An INTERVAL expression if start is a **DATE or TIMESTAMP**, or an integral **numeric** otherwise.
# MAGIC
# MAGIC - **Returns**
# MAGIC   - Column: an **array** of **sequence values**.
# MAGIC
# MAGIC   - By **default** step is **1** if **start is less than or equal to stop**, otherwise **-1**.
# MAGIC
# MAGIC   - For the **DATE or TIMESTAMP** sequences **default** step is **INTERVAL ‘1’ DAY** and **INTERVAL ‘-1’ DAY** respectively.
# MAGIC   
# MAGIC   - If **start is greater than stop** then **step** must be **negative**, and **vice versa**.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, sequence, to_date, lit, explode, expr

# COMMAND ----------

# DBTITLE 1,sample dataframe
# define data
data = [['Sowmya', 29, 'Chennai', '2020-10-25', '2023-01-15', 5, 10],
        ['Bole', 32, 'Bangalore', '2013-10-11', '2029-01-18', 3, 7],
        ['Chandini', 35, 'Hyderabad', '2015-10-17', '2022-04-15', 6, 10],
        ['Deepthi', 40, 'Nasik', '2022-12-21', '2023-04-23', 8, 15],
        ['Swapna', 37, 'Mumbai', '2021-04-14', '2023-07-25', 10, 17],
        ['Tharun', 25, 'Delhi', '2021-06-26', '2021-07-12', 20, 27]] 
  
# define column names
columns = ['emp_name', 'Age', 'City', 'start_date', 'end_date', 'start', 'end'] 
  
# create dataframe using data and column names
df = spark.createDataFrame(data, columns)
df = df.withColumn("start_date", to_date("start_date")) \
       .withColumn("end_date", to_date("end_date"))
  
# view dataframe
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Generating a Sequence of Numbers

# COMMAND ----------

# Generate sequence of numbers
df_value = df.withColumn("num_sequence", sequence(col("start"), col("end")))
display(df_value)

# COMMAND ----------

# Generate sequence of numbers with Step size of 2
df_value_step = df.withColumn("num_sequence", sequence(col("start"), col("end"), lit(2)))
display(df_value_step)

# COMMAND ----------

# MAGIC %md
# MAGIC **Summary**
# MAGIC
# MAGIC | Use Case	   | Example     |
# MAGIC |--------------|-------------|
# MAGIC |Sequence of **Dates**	    |**sequence(to_date(start), to_date(end), lit("1 day"))**.|
# MAGIC |Sequence of **Numbers**    |	**sequence(start, end, step)**|
# MAGIC |Expanding Sequence into **Rows**	| Use **explode(sequence(...))**|

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Generating a Sequence of Dates

# COMMAND ----------

# DBTITLE 1,sequence
df_seq = df.withColumn("date_sequence", F.sequence(F.col("start_date"), F.col("end_date"))) \
           .select("start_date", "end_date", "date_sequence")
display(df_seq)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Generating a Sequence of Months

# COMMAND ----------

# MAGIC %md
# MAGIC **Using expr("INTERVAL 1 DAY") (Recommended)**
# MAGIC      
# MAGIC      # This ensures correct day-wise intervals by using INTERVAL 1 DAY.
# MAGIC      df = df.withColumn("date_sequence", sequence(col("start_date"), col("end_date"), expr("INTERVAL 1 DAY")))
# MAGIC
# MAGIC **Using lit(1) (Alternative)**
# MAGIC
# MAGIC      # This works because sequence() allows step sizes in days when using lit().
# MAGIC      # Unlike months, days have a fixed unit, so lit(1) works the same as "INTERVAL 1 DAY".
# MAGIC      df = df.withColumn("date_sequence", sequence(col("start_date"), col("end_date"), lit(1)))  # 1-day interval
# MAGIC
# MAGIC **Summary**
# MAGIC
# MAGIC | Approach	| Works?	 | Reason       |
# MAGIC |-----------|----------|--------------|
# MAGIC | **expr("INTERVAL 1 DAY")**	| ✅ Yes	| Correctly defines a **1-day interval**|
# MAGIC | **lit(1)** | ✅ Yes	| Works since **days** have **fixed lengths**|
# MAGIC
# MAGIC | Interval Type |	Correct Code |
# MAGIC |---------------|--------------|
# MAGIC | Days (Recommended)	 | **sequence(col("start_date"), col("end_date"), expr("INTERVAL 1 DAY"))** |
# MAGIC | Days (Alternative)	| **sequence(col("start_date"), col("end_date"), lit(1))** |
# MAGIC
# MAGIC - Both methods work, but using **expr("INTERVAL 1 DAY")** is **more explicit** when dealing with PySpark's interval logic. 

# COMMAND ----------

# DBTITLE 1,MONTH
# Generates dates in 1-month increments.
df_seq_month= df\
    .withColumn("month_sequence", F.sequence(F.col("start_date"), F.col("end_date"), expr("INTERVAL 1 MONTH"))) \
    .select("start_date", "end_date", "month_sequence")
display(df_seq_month)

# COMMAND ----------

# DBTITLE 1,WEEK
# Generates dates in 1-month increments.
df_seq_week = df.withColumn("week_sequence", F.sequence(F.col("start_date"), F.col("end_date"), expr("INTERVAL 1 WEEK"))) \
                .select("start_date", "end_date", "week_sequence")
display(df_seq_week)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Expanding Sequence using explode()
# MAGIC - If you want to expand the **sequence** into **multiple rows**, use **explode()**.

# COMMAND ----------

# DBTITLE 1,sequence & explode
df_seq_expl = df.withColumn("seq_date", F.sequence(F.col("start_date"), F.col("end_date")))\
                .withColumn("seq_explode", F.explode(F.sequence(F.col("start_date"), F.col("end_date"))))\
                .select("start_date", "end_date", "seq_date", "seq_explode")   
display(df_seq_expl)

# COMMAND ----------

df_seq_expl_year = df \
    .withColumn("seq_date", F.sequence(F.col("start_date"), F.col("end_date"), expr("INTERVAL 1 YEAR")))\
    .withColumn("seq_explode", F.explode(F.sequence(F.col("start_date"), F.col("end_date"), expr("INTERVAL 1 YEAR"))))\
    .select("start_date", "end_date", "seq_date", "seq_explode")   
display(df_seq_expl_year)

# COMMAND ----------

import datetime

df1 = (
    spark.createDataFrame([{'date':1}])
    .select(
        explode(sequence(
            to_date(lit('2022-01-01')), # start
            to_date(lit(datetime.date.today())), # stop
            expr("INTERVAL 1 WEEK")     # step      # WEEK, MONTH, YEAR
        )).alias('calendar_date')
    )
)

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Spark SQL

# COMMAND ----------

# DBTITLE 1,sequence
spark.sql("SELECT sequence(to_date('2018-01-01'), to_date('2018-06-01'), interval 1 month) as date").display()

# COMMAND ----------

# DBTITLE 1,sequence & explode
spark.sql("SELECT sequence(to_date('2018-01-01'), to_date('2018-06-01'), interval 1 month) as date").withColumn("date", explode(col("date"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sequence(1, 5);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sequence(5, 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sequence(DATE'2018-01-01', DATE'2018-08-01', INTERVAL 1 MONTH);