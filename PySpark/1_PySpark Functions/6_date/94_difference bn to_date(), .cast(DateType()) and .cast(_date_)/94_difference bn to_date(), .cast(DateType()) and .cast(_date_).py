# Databricks notebook source
# MAGIC %md
# MAGIC **Refer below video's in "PySpark playlist" on "to_date()"**
# MAGIC
# MAGIC - 91_date_format( ) | to_date( ) | to_timestamp( ) | date to string & string to date #pyspark PART 91
# MAGIC - 92_How to convert string to date format using to_date? | #pyspark PART 92
# MAGIC - 93_How to convert string, timestamp to date using to_date() | #pyspark PART 93

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1) to_date()

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **to_date()** function is used to format a **"date string" (or) "timestamp string" column** into the **"Date" Type column** using a **specified format**.
# MAGIC
# MAGIC ✅ If the **format is not provide**, to_date() takes the **default value as 'yyyy-MM-dd'**.
# MAGIC
# MAGIC ✅ Extracts only the **date** portion **(removes time part if present)**.
# MAGIC
# MAGIC ✅ Returns **NULL** if the format does **not match**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax:**
# MAGIC
# MAGIC      to_date(column,format)
# MAGIC      to_date(col("string_column"),"MM-dd-yyyy")

# COMMAND ----------

# MAGIC %md
# MAGIC **Example:**
# MAGIC
# MAGIC ##### to_date()
# MAGIC
# MAGIC 1) Converting a **String** Column with **Default Format**.
# MAGIC    - If your **date strings** follow the **default** format **"yyyy-MM-dd"**, you can simply apply **to_date without specifying a format**.
# MAGIC
# MAGIC 2) Converting a **String** Column with a **Custom Format**
# MAGIC
# MAGIC    - If your **date strings** are in a **different** format (e.g., **"MM/dd/yyyy"**), you **must specify the format** in the **to_date** function.
# MAGIC
# MAGIC |      col_name	        |         format                   | default format: yyyy-MM-dd  |  After to_date(col_name, "yyyy-MM-dd") | correct format |
# MAGIC |-----------------------|----------------------------------|-----------------------------|----------------------------------------|----------------|
# MAGIC | "2024-03-06"	        |  to_date("2024-03-06")           |      Matching               |         2024-03-06 (Date)              | to_date("2024-03-06") |
# MAGIC | "06-03-2024"	        |  to_date("06-03-2024")           |      Not Matching           |       NULL (Format mismatch)           | to_date("06-03-2024", "dd-MM-yyyy") |
# MAGIC | "2024-03-06 12:30:00" |	 to_date("2024-03-06 12:30:00")  |      Not Matching           |      2024-03-06 (Time removed)         | to_date("2024-03-06 12:30:00", "yyyy-MM-dd HH:mm:ss") |

# COMMAND ----------

# DBTITLE 1,import required functions
from pyspark.sql.functions import to_date, col

# COMMAND ----------

# MAGIC %md
# MAGIC #### Converting a "String" Column with "Default Format"
# MAGIC
# MAGIC - If your **date strings** follow the **default** format **"yyyy-MM-dd"**, you can simply apply **to_date without specifying a format**.

# COMMAND ----------

# Sample data with date in "MM/dd/yyyy" format
data = [("2021-12-01", "12/01/2021", "25/04/2023 2:00", "6-Feb-23", "2021-07-24 12:01:19.335"),
        ("2022-01-15", "01/15/2022", "26/04/2023 6:01", "1-Mar-22", "2019-07-22 13:02:20.220"),
        ("2023-03-20", "03/20/2023", "20/01/2020 4:01", "9-Apr-24", "2021-07-25 03:03:13.098"),
        ("2024-06-28", "05/25/2024", "26/04/2023 2:02", "8-May-20", "2023-09-25 15:33:43.054"),
        ("2025-09-12", "07/20/2025", "25/04/2023 5:02", "7-Jun-21", "2024-05-25 23:53:53.023"),
        ("2025-03-22", "09/29/2020", "25/04/2023 9:03", "5-Jul-23", "2024-04-12 13:33:53.323")]

columns = ["ts_format_01", "ts_format_02", "ts_format_03", "ts_format_04", "ts_format_05"]

# Create DataFrame
df_custom = spark.createDataFrame(data, columns)

# Convert string column to date type using custom format
df_with_date = df_custom\
    .withColumn("date_parsed_01", to_date(col("ts_format_01"))) \
    .withColumn("date_parsed_02", to_date(col("ts_format_02"), "MM/dd/yyyy")) \
    .withColumn("date_parsed_03", to_date(col("ts_format_03"), "dd/MM/yyyy H:mm")) \
    .withColumn("date_parsed_04", to_date(col("ts_format_04"), "d-MMM-yy")) \
    .withColumn("date_parsed_05", to_date(col("ts_format_05")))
display(df_with_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) .cast(DateType())

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Features:**
# MAGIC
# MAGIC ✅ Converts **string or timestamp** into a **date**, but assumes **"yyyy-MM-dd"** format.
# MAGIC
# MAGIC ✅ If the format **does not match**, it returns **NULL**.
# MAGIC
# MAGIC ✅ Equivalent to **.cast("date")**, but more explicit in code.
# MAGIC
# MAGIC ✅ **Cannot** specify a **custom format** like **to_date()**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Example:**
# MAGIC
# MAGIC | col_name	 | After col_name.cast(DateType()) |
# MAGIC |------------|---------------------------------|
# MAGIC | "2024-03-06"	         | 2024-03-06 (Date)        |
# MAGIC | "06-03-2024"	         | NULL (Format mismatch)   |
# MAGIC | "2024-03-06 12:30:00"	 | 2024-03-06 (Time removed)|

# COMMAND ----------

# MAGIC %md
# MAGIC **a) Convert "String" column to "DateType"**
# MAGIC - If a DataFrame contains a column with **date** values as **strings**, you can **cast** it to **DateType()**.

# COMMAND ----------

# MAGIC %md
# MAGIC      df = df.withColumn("date_column", col("date_string").cast(DateType()))
# MAGIC                                     (or)
# MAGIC      df_cast = df.withColumn("date_cast", df.date_string.cast(DateType()))
# MAGIC                                     (or)
# MAGIC      df = df.withColumn('date_col', df['date_str'].cast(DateType()))                               

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import DateType

# Sample Data
data = [("2025-03-29",), ("2024-06-15",), ("2023-01-01",), ("2023-06-11",), ("2022-09-21",)]
df_date = spark.createDataFrame(data, ["date_string"])

# Convert the String column to DateType
df_date_type = df_date.withColumn("date_column", col("date_string").cast(DateType()))
display(df_date_type)

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Convert "Timestamp String" Column to "DateType"**
# MAGIC - If a column contains **TimestampType** values, casting it to **DateType()** will **remove** the **time portion**.
# MAGIC - If your DataFrame has a **timestamp string** column (for example, **'yyyy-MM-dd HH:mm:ss'**) and you only need the **date** portion, you can **cast** it to **DateType**.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_ts = spark.createDataFrame([(1,)], ["id"])

# Add a Timestamp Column
df_ts_curr = df_ts.withColumn("timestamp_col", current_timestamp())

# Convert to DateType
df_ts_curr_date = df_ts_curr.withColumn("date_col", col("timestamp_col").cast(DateType()))
display(df_ts_curr_date)

# COMMAND ----------

from pyspark.sql import functions as F

# Sample data with timestamp strings
data = [("2025-03-29 14:30:45",),
        ("2025-04-01 08:15:00",),
        ("2024-06-11 18:25:55",),
        ("2023-08-17 22:55:35",),
        ("2025-04-01 20:45:22",)]
        
columns = ["timestamp_str"]

# Create DataFrame
df_ts_str = spark.createDataFrame(data, columns)

# Cast the timestamp string column to DateType
df_ts_cast = df_ts_str.withColumn("date_only", col("timestamp_str").cast(DateType()))
display(df_ts_cast)

# COMMAND ----------

# MAGIC %md
# MAGIC **c) Convert "Custom Format String" to "DateType"**
# MAGIC - If the **string** format is **not yyyy-MM-dd, to_date()** is needed before **casting**.

# COMMAND ----------

from pyspark.sql.functions import to_date

data = [("03-29-2025",),
        ("06-15-2024",),
        ("09-25-2023",),
        ("02-10-2022",),
        ("04-22-2024",),
        ("07-03-2021",)]

df_na = spark.createDataFrame(data, ["date_string"])

# Convert to DateType
df_na_dt = df_na.withColumn("date_column", to_date(col("date_string"), "MM-dd-yyyy").cast(DateType()))
display(df_na_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **d) Using a "Literal" and Casting to "DateType"**
# MAGIC - You can also use a **literal** value and **cast** it directly to a **DateType**.

# COMMAND ----------

# Use a literal timestamp string and cast it to DateType
df_literal = spark.range(8)\
    .select(F.lit("2025-03-29 14:30:45").cast(DateType()).alias("date_val"))
display(df_literal)

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Takeaways**
# MAGIC - Direct Cast **(.cast(DateType()))** works when the column is already in **yyyy-MM-dd or TimestampType**.
# MAGIC - For **custom formats**, use **to_date()** before **casting**.
# MAGIC - **Time** information is **removed** when **casting** from **TimestampType to DateType**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) .cast("date")
# MAGIC
# MAGIC - In Spark, **.cast("date")** is a **shorthand** way to **cast** a column to **DateType**.
# MAGIC - It is commonly used when working with **string timestamps** that need to be converted into **DateType**.
# MAGIC - Works directly on **yyyy-MM-dd** formatted **strings** but may require **to_date()** for **other formats**.
# MAGIC - **Removes** the **time** portion from **TimestampType**, keeping **only the date**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Features:**
# MAGIC
# MAGIC ✅ Identical to **.cast(DateType())**, but uses a string notation.
# MAGIC
# MAGIC ✅ Converts **string or timestamp** to **date**, assuming **"yyyy-MM-dd"** format.
# MAGIC
# MAGIC ✅ **Cannot** handle **different formats** like **to_date()**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Example:**
# MAGIC
# MAGIC | col_name	  | After col_name.cast("date")  |
# MAGIC |-------------|------------------------------|
# MAGIC | "2024-03-06"	 | 2024-03-06 (Date)  |
# MAGIC | "06-03-2024"	 | NULL (Format mismatch)  |
# MAGIC | "2024-03-06 12:30:00"	 | 2024-03-06 (Time removed) |

# COMMAND ----------

# MAGIC %md
# MAGIC **a) Casting a "String" Column to "DateType"**
# MAGIC
# MAGIC - If a column contains **date strings** in the format **yyyy-MM-dd**, you can directly **cast** it to **DateType**.
# MAGIC - You can **cast** a **string to DateType** directly using **.cast("date")**.
# MAGIC - The input **string** should be in a **recognizable date format**, usually **yyyy-MM-dd**.

# COMMAND ----------

# DBTITLE 1,convert string column to DateType
from pyspark.sql.functions import col

# Create DataFrame with date strings
df_cast = spark.createDataFrame([("2023-12-25",),
                                 ("2024-01-01",),
                                 ("2020-03-11",),
                                 ("2021-04-21",),
                                 ("2022-06-09",)],
                                ["date_str"])

# Cast string column to DateType
df_cast_dt = df_cast.withColumn("date_col", col("date_str").cast("date"))
display(df_cast_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Casting a "Timestamp" Column to "DateType"**
# MAGIC
# MAGIC - If you have a **TimestampType** column and want to extract only the **date** part.

# COMMAND ----------

# DBTITLE 1,convert timestamp column to DateType, removing time part.
from pyspark.sql.functions import current_timestamp

# Create DataFrame with current timestamp
df_ts_dt = spark.createDataFrame([(1,)], ["id"])

df_ts_dt_ts = df_ts_dt.withColumn("timestamp_col", current_timestamp())

# Cast TimestampType to DateType
df_ts_dt_ts_final = df_ts_dt_ts.withColumn("date_col", col("timestamp_col").cast("date"))
display(df_ts_dt_ts_final)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Create a DataFrame with the current timestamp
df_ts_cts = spark.range(1)\
  .withColumn("current_ts", current_timestamp())

# Casting the timestamp column to date
df_ts_with_date = df_ts_cts.withColumn("date_col", col("current_ts").cast("date"))
display(df_ts_with_date)

# COMMAND ----------

# MAGIC %md
# MAGIC **c) Casting a "String" Column with "Custom Format"**
# MAGIC
# MAGIC - If the **date string** is in a **non-default** format, first use **to_date()** before **casting**.
# MAGIC
# MAGIC - If you have a **string** in a **custom format (MM-dd-yyyy)**, and you want to **cast** it to **DateType**, you first need to **convert** the **string into a TimestampType or DateType** using functions like **to_date()**. Once the **string** is in the **correct format**, you can use **.cast("date")**.

# COMMAND ----------

# DBTITLE 1,input format is MM-dd-yyyy, to_date() is used before casting
from pyspark.sql.functions import to_date

# Create DataFrame with custom date format
df_cust_date1 = spark.createDataFrame([("06-24-2019",),
                                       ("03-14-2020",),
                                       ("05-12-2021",),
                                       ("07-17-2022",),
                                       ("08-04-2023",)], ["date_str"])

# Convert to DateType using to_date() and then cast (optional)
df_cust_date1_cast = df_cust_date1\
  .withColumn("date_col", to_date(col("date_str"), "MM-dd-yyyy").cast("date"))
  
display(df_cust_date1_cast)

# COMMAND ----------

# MAGIC %md
# MAGIC **d) Casting a Column from Integer to Date**
# MAGIC
# MAGIC - If you have an **integer** representing a **date**, such as **20210625**, and you want to **convert** it to a **DateType**, you can first convert the **integer to a string** and then **cast** it to **DateType**.

# COMMAND ----------

# DBTITLE 1,convert "integer" to "date"
from pyspark.sql.functions import col, expr

# Sample DataFrame with date as integer
df_expr = spark.createDataFrame([(20210625,),
                                 (20230710,),
                                 (20210415,),
                                 (20220318,),
                                 (20250928,),], ['date_int'])

# Convert integer to string -> Reformat to yyyy-MM-dd -> Cast to DateType
df_expr_dt = df_expr\
  .withColumn("date_col", expr("to_date(cast(date_int as string), 'yyyyMMdd')"))

display(df_expr_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **e) Using .cast("date") with a Column of Dates**
# MAGIC
# MAGIC - If you have a DataFrame with **dates** already in **string** format but need to explicitly **cast** them to **DateType**.

# COMMAND ----------

# DBTITLE 1,convert "date string" to "date"
# Sample DataFrame with string dates
df_dt_str = spark.createDataFrame([('2021-01-01',),
                                   ('2022-02-14',),
                                   ('2023-04-21',),
                                   ('2025-06-21',),
                                   ('2025-09-19',)], ['date_str'])

# Cast string column to DateType
df_dt_str_type = df_dt_str.withColumn('date_col', col('date_str').cast("date"))
display(df_dt_str_type)

# COMMAND ----------

# MAGIC %md
# MAGIC **f) Using .cast("date") in SQL Expressions**
# MAGIC
# MAGIC - You can also use **.cast("date")** within **selectExpr()**.

# COMMAND ----------

# DBTITLE 1,"Timestamp string" has been converted to "DateType", keeping only "YYYY-MM-DD"
df_selectexp = spark.createDataFrame([("2024-07-15 10:30:00",),
                                      ("2025-09-25 20:55:55",),
                                      ("2023-10-19 13:43:23",),
                                      ("2022-12-29 23:59:19",),
                                      ("2021-04-12 19:51:27",)], ["timestamp_str"])

# Using selectExpr to cast column
df_selectexp_cast = df_selectexp.selectExpr("timestamp_str as original", "cast(timestamp_str as date) as date_col")
display(df_selectexp_cast)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Difference between ".cast(DateType())" and ".cast("date")"

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Differences at a Glance**
# MAGIC
# MAGIC | Feature	   | .cast(DateType())	 | .cast("date") |
# MAGIC |------------|---------------------|---------------|
# MAGIC | Explicit Import?	| Yes (**from pyspark.sql.types import DateType**)	| **No** |
# MAGIC | Data Type Usage? | Uses PySpark **DateType()** object	| Uses string **"date"** |
# MAGIC | Readability?	| More explicit |	More concise |
# MAGIC | Error Handling?	| **fails if input format is incorrect**	| Same behavior |
# MAGIC | Use Case?	| **Schema definition**, strict typing |	**Quick conversions** |

# COMMAND ----------

# MAGIC %md
# MAGIC **a) ".cast(DateType())" (Using PySpark DataType)**
# MAGIC
# MAGIC - Explicitly uses the **DateType** from **pyspark.sql.types**.
# MAGIC
# MAGIC - Requires **importing DateType** from **pyspark.sql.types**.
# MAGIC
# MAGIC - Used when **defining schema** or explicitly specifying the **data type**.

# COMMAND ----------

from pyspark.sql.types import DateType
from pyspark.sql.functions import col

df = spark.createDataFrame([('2023-06-15',),
                            ('2024-03-25',),
                            ('2023-07-19',),
                            ('2023-11-15',),
                            ('2022-04-09',)], ['date_str'])

# Using DateType()
df = df.withColumn("date_col", col("date_str").cast(DateType()))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **b) ".cast("date")" (Using String Representation)**
# MAGIC
# MAGIC - Uses the **string "date"** as an **alias** for **DateType**.
# MAGIC
# MAGIC - **No** need to **import DateType**.
# MAGIC
# MAGIC - Internally, **Spark converts "date" to DateType()**.

# COMMAND ----------

df = spark.createDataFrame([('2021-02-25',),
                            ('2023-06-12',),
                            ('2025-04-14',),
                            ('2024-09-19',),
                            ('2020-08-17',)], ['date_str'])

# Using "date"
df = df.withColumn("date_col", col("date_str").cast("date"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Difference between "to_date()", ".cast(DateType())" and ".cast("date")"

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Differences:**
# MAGIC
# MAGIC | Feature	 | to_date()	 | .cast(DateType()) / .cast("date") |
# MAGIC |----------|-------------|--------------------------|
# MAGIC | Purpose	 | Converts **string to date** using a **specific format**	 | Converts to **date** assuming **"yyyy-MM-dd"** |
# MAGIC | Format  | Handling	Supports **custom formats (yyMMdd, dd-MM-yyyy)**	| Works only for **"yyyy-MM-dd"** |
# MAGIC | Time Removal	| **Removes time part**	| **Removes time part** |
# MAGIC | Error Handling	| Returns **NULL** if **format doesn't match**	| Returns **NULL** if **format doesn't match** |
# MAGIC | Recommended Use	| When working with **various date formats**	| When the **format** is already **"yyyy-MM-dd"** |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Best Practices
# MAGIC
# MAGIC - Use **to_date(col, format)** if your **date format varies**.
# MAGIC
# MAGIC - Use **.cast(DateType()) or .cast("date")** if the column is already in **"yyyy-MM-dd"** format.
# MAGIC
# MAGIC - Avoid **.cast()** if your column has **different formats**, use **to_date()** instead.