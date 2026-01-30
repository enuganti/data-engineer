# Databricks notebook source
# MAGIC %md
# MAGIC #### datediff() and months_between()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) datediff
# MAGIC - Difference between two dates (days, months, years)
# MAGIC - `datediff` is used to calculate the **date difference** between **two dates** in terms of **DAYS**.
# MAGIC - To compute the duration between **two timestamps or date values**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Syntax**
# MAGIC      datediff(endDate, startDate)
# MAGIC
# MAGIC - This function takes the **end date** as the **first argument** and the **start date** as the **second argument** and returns the **number of days** in between them.

# COMMAND ----------

# DBTITLE 1,Sample Dataframe 01
# define data
data = [['Sowmya', 29, 'Chennai', '2020-10-25', '2023-01-15', '2021-11-20', '2022-05-11'],
        ['Bole', 32, 'Bangalore', '2013-10-11', '2029-01-18', '2012-08-17', '2028-04-28'],
        ['Chandini', 35, 'Hyderabad', '2015-10-17', '2022-04-15', '2017-12-27', '2023-04-05'],
        ['Deepthi', 40, 'Nasik', '2022-12-21', '2023-04-23', '2023-02-26', '2024-08-20'],
        ['Swapna', 37, 'Mumbai', '2021-04-14', '2023-07-25', '2022-09-24', '2025-04-22'],
        ['Tharun', 25, 'Delhi', '2021-06-26', '2021-07-12', '2023-06-17', '2025-09-22']] 
  
# define column names
columns = ['emp_name', 'Age', 'City', 'start_date', 'end_date', 'purchase_date', 'delivery_date'] 
  
# create dataframe using data and column names
df_diff = spark.createDataFrame(data, columns)
  
# view dataframe
display(df_diff)

df_diff.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, floor, to_date, to_timestamp, current_date, datediff, months_between, round, lit, unix_timestamp

# COMMAND ----------

df_diff = df_diff.withColumn("start_date", to_date("start_date")) \
                 .withColumn("end_date", to_date("end_date"))

df_diff.printSchema()

# COMMAND ----------

# create new DataFrame with date differences columns
df_diff_01 = df_diff.withColumn('diff_days', F.datediff(col('end_date'), col('start_date')))
display(df_diff_01)

# COMMAND ----------

# MAGIC %md
# MAGIC - The are **812 days** between `2020-10-25 and 2023-01-15`.
# MAGIC - The are **5578 days** between `2029-01-18 and 2013-10-11`.
# MAGIC - The are **2372 days** between `2022-04-15 and 2015-10-17`.
# MAGIC - The are **123 days** between `2023-04-23 and 2022-12-21`.
# MAGIC - The are **832 days** between `2023-07-25 and 2021-04-14`.
# MAGIC - The are **16 days** between `2021-07-12 and 2021-06-26`.

# COMMAND ----------

# Calculate the difference between two dates
df_diff_02 = df_diff.select(current_date().alias("current_date"),
                            col("start_date"),
                            datediff(current_date(), col("start_date")).alias("datediff_in_days")
                    )
display(df_diff_02)

df_diff_02.printSchema()

# COMMAND ----------

# Calculate Difference Between Dates in Days
df_diff_02 = df_diff.withColumn('diff_days', F.datediff(F.to_date('delivery_date'), F.to_date('purchase_date')))
display(df_diff_02)
df_diff_02.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) months_between()
# MAGIC
# MAGIC - Date Difference in Months
# MAGIC - Date Difference in Seconds
# MAGIC   - Using unix_timestamp()
# MAGIC   - Using to_timestamp()
# MAGIC - Date Difference in Minutes
# MAGIC - Date Difference in Hours

# COMMAND ----------

# MAGIC %md
# MAGIC ##### a) Date Difference in Months

# COMMAND ----------

# MAGIC %md
# MAGIC ##### months_between():
# MAGIC   - Returns **number of months** between dates **date1 and date2**.
# MAGIC   - If `date1` is later than `date2`, then the result is `positive`.
# MAGIC   - A whole number is returned if both inputs have the same day of month or both are the last day of their respective months.
# MAGIC   - The difference is calculated assuming **31 days per month**.
# MAGIC   - The result is **rounded off** to **8 digits** unless **roundOff** is set to **False**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Syntax**
# MAGIC
# MAGIC       Syntax: months_between(end_date, start_date)
# MAGIC       
# MAGIC          Returns => number of months between two dates.
# MAGIC
# MAGIC |    Parameter Name	      |  Required  | Description                                            |
# MAGIC |--------------------------|------------|--------------------------------------------------------|
# MAGIC | end_date (str, Column)   |    Yes	    | It represents the ending date.                         |
# MAGIC | start_date (str, Column) |    Yes	    | It represents the starting date.                       |
# MAGIC | roundOff (bool)	         | Optional	 | It represents the difference to be rounded off or not. |
# MAGIC
# MAGIC - **roundOffbool**, optional => whether to **round** (to **8 digits**) the final value or not `(default: True)`.

# COMMAND ----------

# DBTITLE 1,Sample Dataframe 01
data = [("2019-01-11", "2021-04-12", "2019-09-17 12:02:21", "2021-07-12 18:29:29"),
        ("2019-08-04", "2021-04-15", "2018-11-11 14:17:05", "2021-08-03 16:21:40"),
        ("2019-03-24", "2021-02-08", "2019-02-07 04:26:49", "2020-11-28 05:20:33"),
        ("2019-04-13", "2021-06-05", "2019-07-08 20:04:09", "2021-05-18 08:21:12"),
        ("2019-02-22", "2021-10-01", "2018-11-28 05:46:54", "2021-06-17 21:39:42")
       ]
 
columns = ["from_date", "to_date", "from_datetime", "to_datetime"]

df_samp01 = spark.createDataFrame(data, schema=columns)
display(df_samp01)

df_samp01.printSchema()

# COMMAND ----------

df_samp01 = df_samp01.withColumn("from_date", to_date("from_date")) \
                     .withColumn("to_date", to_date("to_date")) \
                     .withColumn("from_datetime", to_timestamp("from_datetime", "yyyy-MM-dd HH:mm:ss")) \
                     .withColumn("to_datetime", to_timestamp("to_datetime", "yyyy-MM-dd HH:mm:ss"))

display(df_samp01)

df_samp01.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **How to find the month difference between days?**

# COMMAND ----------

# DBTITLE 1,using select
df1 = df_samp01.select("from_date",
                       months_between("to_date", "from_date").alias("months_between"),
                       floor(months_between("to_date", "from_date")).alias("months_between_floor"),
                       "to_date")
                       
display(df1)

# COMMAND ----------

# DBTITLE 1,using withColumn
df2 = df_samp01.withColumn("months_between", floor(months_between("to_datetime", "from_datetime"))) \
               .select("to_datetime", "months_between", "from_datetime")
               
display(df2)

# COMMAND ----------

df22 = df_samp01.withColumn("months_between", months_between("to_datetime", "to_date")) \
                .withColumn("months_between_False", months_between("to_datetime", "to_date", False)) \
                .withColumn("months_between_True", months_between("to_datetime", "to_date", True)) \
                .withColumn("months_between_floor", floor(months_between("to_datetime", "to_date"))) \
                .select("to_datetime", "months_between", "months_between_False", "months_between_True", "months_between_floor", "to_date")
               
display(df22)

# COMMAND ----------

# DBTITLE 1,Sample Dataframe 02
# Create DataFrame
data = [("1", "2019-07-01"),
        ("2", "2019-06-24"),
        ("3", "2019-08-24"),
        ("4", "2019-09-24"),
        ("5", "2019-10-24")
        ]

df_samp02 = spark.createDataFrame(data=data, schema=["id", "date"])
df_samp02.printSchema()

df_samp02 = df_samp02.withColumn("date", to_date("date"))
display(df_samp02)
df_samp02.printSchema()

# COMMAND ----------

# Calculate the difference between two dates in months
df3 = (df_samp02.withColumn("diff_months_def", months_between(current_date(), col("date")))
                .withColumn("diff_months_True", months_between(current_date(), col("date"), True))     # round (to 8 digits)
                .withColumn("diff_months_False", months_between(current_date(), col("date"), False))
                .withColumn("diff_months_round", F.round(months_between(current_date(), col("date")), 1))
                .withColumn("diff_months_floor", floor(months_between(current_date(), col("date")))))
  
display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark SQL**

# COMMAND ----------

df_samp01.createOrReplaceTempView("days")

# COMMAND ----------

spark.sql("""
SELECT
    from_date,
    floor(months_between(to_date, from_date)) AS months_between,
    to_date
FROM days
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT months_between('1997-02-28 10:30:00', '1996-10-30') AS Months_True;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT months_between('1997-02-28 10:30:00', '1996-10-30', false) AS Months_False;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Get Differences Between Dates in Years

# COMMAND ----------

# DBTITLE 1,years
# Calculate the difference between two dates in years
df4 = df_samp02.withColumn("diff_years", F.round(months_between(current_date(), col("date"))/12, 1)) \
               .withColumn("diff_years_roundoff", F.round(months_between(current_date(), col("date")) / lit(12), 1))
display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### c) Date Difference in Hours / Minutes

# COMMAND ----------

# MAGIC %md
# MAGIC      df.withColumn("ux_current_date", unix_timestamp(col("current_date")))
# MAGIC                                      (or)
# MAGIC      df.withColumn("ux_current_date", unix_timestamp(current_date()))

# COMMAND ----------

df5 = df_samp02.withColumn("ux_current_date", unix_timestamp(col("current_date"))) \
               .withColumn("ux_date", unix_timestamp(col("date"))) \
               .withColumn("seconds_between", unix_timestamp(current_date()) - unix_timestamp(col("date"))) \
               .withColumn("minutes_between", col("seconds_between")/60) \
               .withColumn("hours_between", col("minutes_between")/60)

display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### d) Date Difference in Seconds

# COMMAND ----------

# MAGIC %md
# MAGIC **i) Using unix_timestamp()**
# MAGIC
# MAGIC ##### **Syntax:**
# MAGIC      unix_timestamp(timestamp, TimestampFormat)
# MAGIC
# MAGIC **Note:** The UNIX timestamp function converts the timestamp into the **number of seconds since the first of January 1970**.

# COMMAND ----------

df6 = df_samp02.withColumn("ux_current_date", unix_timestamp(col("current_date"))) \
               .withColumn("ux_date", unix_timestamp(col("date"))) \
               .withColumn("seconds_between", unix_timestamp(col("current_date")) - unix_timestamp(col("date")))

display(df6)

# COMMAND ----------

# MAGIC %md
# MAGIC **ii) Using to_timestamp()**
# MAGIC
# MAGIC ##### Syntax:
# MAGIC      to_timestamp(timestamp, format])

# COMMAND ----------

# unix_timestamp() function to convert timestamps to seconds
# to_timestamp(col("current_date")) => Ensures the column is treated as a timestamp
# unix_timestamp() => Converts the timestamp to seconds
# time difference in "seconds" between "current_date" and "date"

df7 = df_samp02.withColumn("date_ts", to_timestamp(col("date"))) \
               .withColumn("current_date", current_date()) \
               .withColumn("current_date_ts", to_timestamp(current_date())) \
               .withColumn("seconds_between_to_timestamp",
                           unix_timestamp(to_timestamp(col("current_date"))) - unix_timestamp(to_timestamp(col("date"))))

display(df7)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### e) Calculating Differences when Dates are in Custom Format
# MAGIC
# MAGIC - Difference between two dates when dates are `not in DateType format yyyy-MM-dd`.
# MAGIC - When dates are `not in DateType format`, all date functions return `null`.
# MAGIC - Hence, first `convert` the input date to DateType using `to_date()` function and then calculate the `differences`.

# COMMAND ----------

# Calculate Difference Between Dates in Months
df8 = df_samp01.withColumn('diff_months', F.round(F.months_between(F.to_date('to_date'), F.to_date('from_date')),2))
display(df8)

# COMMAND ----------

# Calculate Difference Between Dates in Years
df9 = df_samp01.withColumn('diff_years', F.round(F.months_between(F.to_date('to_date'), F.to_date('from_date'))/12,2))
display(df9)