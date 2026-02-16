# Databricks notebook source
# MAGIC %md
# MAGIC #### to_date()
# MAGIC
# MAGIC ✅ **to_date()** function is used to format a **"date string" (or) "timestamp string" column** into the **"Date" Type column** using a **specified format**.
# MAGIC
# MAGIC ✅ If the **format is not provided**, to_date() takes the **default value as 'yyyy-MM-dd'**.
# MAGIC
# MAGIC ✅ Extracts only the **date** portion **(removes time part if present)**.
# MAGIC
# MAGIC ✅ Returns **NULL** if the format does **not match**.

# COMMAND ----------

# MAGIC %md
# MAGIC - **to_date():** extracts **only the date** part (ignores time).
# MAGIC - **to_timestamp():** parses **both date and time**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax:**
# MAGIC
# MAGIC      to_date(column,format)

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

# MAGIC %md
# MAGIC #### 1) Converting a "String" Column with "Default Format"
# MAGIC
# MAGIC - If your **date strings** follow the **default** format **"yyyy-MM-dd"**, you can simply apply **to_date**, **without specifying a format**.

# COMMAND ----------

# DBTITLE 1,import required functions
from pyspark.sql.functions import to_date, col

# COMMAND ----------

# DBTITLE 1,transform "date string" to "date"
# Sample data with date in default format "yyyy-MM-dd"
data = [("2021-12-01",),
        ("2022-01-15",),
        ("2023-03-20",),
        ("2024-06-28",),
        ("2025-09-12",),
        ("2025-03-22",)]

columns = ["date_string"]

# Create DataFrame
df_default = spark.createDataFrame(data, columns)

# Convert string column to date type
df_with_date = df_default.withColumn("date_parsed", to_date(col("date_string")))
display(df_with_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Converting a "String" Column with a "Custom Format"
# MAGIC
# MAGIC - If your **date strings** are in a **different** format (e.g., **"MM/dd/yyyy"**), you **must specify the format** in the **to_date** function.
# MAGIC - Suppose you have **multiple columns** with **dates in different formats**.
# MAGIC - You can **convert** each one separately by applying **to_date** with the **corresponding format**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 01**

# COMMAND ----------

# DBTITLE 1,custom format: Example 01
# Sample data with date in "MM/dd/yyyy" format
data = [("12/01/2021", "25/04/2023 2:00", "6-Feb-23", "2021-07-24 12:01:19.335"),
        ("01/15/2022", "26/04/2023 6:01", "1-Mar-22", "2019-07-22 13:02:20.220"),
        ("03/20/2023", "20/01/2020 4:01", "9-Apr-24", "2021-07-25 03:03:13.098"),
        ("05/25/2024", "26/04/2023 2:02", "8-May-20", "2023-09-25 15:33:43.054"),
        ("07/20/2025", "25/04/2023 5:02", "7-Jun-21", "2024-05-25 23:53:53.023"),
        ("09/29/2020", "25/04/2023 9:03", "5-Jul-23", "2024-04-12 13:33:53.323")]

columns = ["ts_format_01", "ts_format_02", "ts_format_03", "ts_format_04"]

# Create DataFrame
df_custom = spark.createDataFrame(data, columns)

# Convert string column to date type using custom format
df_with_date = df_custom\
    .withColumn("date_parsed_01", to_date(col("ts_format_01"), "MM/dd/yyyy")) \
    .withColumn("date_parsed_02", to_date(col("ts_format_02"), "dd/MM/yyyy H:mm")) \
    .withColumn("date_parsed_03", to_date(col("ts_format_03"), "d-MMM-yy")) \
    .withColumn("date_parsed_04", to_date(col("ts_format_04"))) # default format yyyy-MM-dd HH:
display(df_with_date)

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 02**

# COMMAND ----------

# DBTITLE 1,custom format dataframe
data1 = [("2025-03-26", "26-03-2025", "02/12/2022", "6-Feb-23", "invalid_date", "06-03-2019"),
         ("2024-12-31", "31-12-2024", "04/15/2023", "8-Jan-24", "invalid_date", "16-04-2020"),
         ("2023-07-15", "15-07-2023", "06/18/2024", "6-Mar-23", "invalid_date", "26-05-2021"),
         ("2022-05-25", "25-03-2020", "07/20/2025", "7-Jan-25", "invalid_date", "14-06-2022"),
         ("1998-08-05", "08-05-1982", "08/22/2020", "8-Apr-23", None, "19-11-2023"),
         ("1995-09-29", "29-09-2021", "09/29/2010", "9-Feb-25", None, "29-12-2024"),
         ("1995-09-29", "29-09-2021", "09/29/2010", "9-Feb-25", "2025-03-25", "29-12-2024"),
         ("1995-09-29", "29-09-2021", "09/29/2010", "9-Feb-25", "2024-09-12", "29-12-2024"),
         ("1995-09-29", "29-09-2021", "09/29/2010", "9-Feb-25", "2020-11-29", "29-12-2024")
        ]
columns1 = ["d1", "d2", "d3", "d4", "d5", "d6"]

df_custom_01 = spark.createDataFrame(data1, columns1)
display(df_custom_01)

# COMMAND ----------

# DBTITLE 1,Convert default & custom format columns
# Convert each string column to date type with appropriate format
from pyspark.sql.functions import col, to_date, when

df1_with_date = (
    df_custom_01
    .withColumn("d1_custom", to_date(col("d1")))  # default yyyy-MM-dd
    .withColumn("d2_custom", to_date(col("d2"), "dd-MM-yyyy"))
    .withColumn("d3_custom", to_date(col("d3"), "MM/dd/yyyy"))
    .withColumn("d4_custom", to_date(col("d4"), "d-MMM-yy"))
    .withColumn(
        "d5_custom",
        when(col("d5").isNotNull() & (col("d5") != "invalid_date"),
             to_date(col("d5"), "yyyy-MM-dd"))
        .otherwise(None)
    )
    .withColumn("d6_custom", to_date(col("d6"), "dd-MM-yyyy"))
)

display(df1_with_date)

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 03**

# COMMAND ----------

# DBTITLE 1,custom format dataframe
data2 = [("25/04/2023 2:00", "25/04/2023 2", "25/04/2023 22:56:18", "2021-07-24 12:01:19.335"),
         ("26/04/2023 6:01", "26/04/2023 6", "25/04/2002 21:12:00", "2019-07-22 13:02:20.220"),
         ("20/01/2020 4:01", "20/01/2020 4", "25/04/1957 20:12:01", "2021-07-25 03:03:13.098"),
         ("26/04/2023 2:02", "26/04/2023 2", "25/04/2023 23:45:22", "2023-09-25 15:33:43.054"),
         ("25/04/2023 5:02", "25/04/2023 5", "25/04/2024 14:12:03", "2024-05-25 23:53:53.023"),
         ("26/03/2023 8:04", "26/03/2023 8", "25/05/2021 23:45:04", "2025-03-25 22:43:33.323")
        ]
columns2 = ["t1", "t2", "t3", "t4"]

df_custom_02 = spark.createDataFrame(data2, columns2)
display(df_custom_02)

# COMMAND ----------

# DBTITLE 1,Convert default & custom format columns
df2_with_date = (df_custom_02\
    .withColumn("t1_custom", to_date(col("t1"), 'dd/MM/yyyy H:mm'))
    .withColumn('t2_custom', to_date(col("t2"), 'dd/MM/yyyy H'))
    .withColumn("t3_custom", to_date(col("t3"), 'dd/MM/yyyy HH:mm:ss')) # extracts only date portion (removes time)
    .withColumn("t4_custom", to_date(col("t4"))) # extracts only date portion (removes time)
)
    
display(df2_with_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Using to_date() with selectExpr()
# MAGIC
# MAGIC - to convert the **datetime string** to a **date**, **ignoring the time** part.
# MAGIC
# MAGIC

# COMMAND ----------

# Sample data with custom format
data = [("2021-12-01 12:30:00", "2021-12-01"),
        ("2022-01-15 10:00:00", "2025-06-15"),
        ("2023-03-20 14:45:00", "2019-09-25"),
        ("2024-09-25 19:55:45", "2024-12-30"),
        ("2025-02-15 11:45:25", "2025-04-28"),
        ("2020-12-25 16:25:55", "2023-10-18")]

columns = ["datetime_string", "input_date"]

# Create DataFrame
df_custom_03 = spark.createDataFrame(data, columns)

# Convert string with datetime to date type using selectExpr
df_custom_expr = df_custom_03.selectExpr(
  "to_date(datetime_string) as date", # extracts only the date from the timestamp.
  "to_date(datetime_string, 'yyyy-MM-dd HH:mm:ss') as date_time", # extracts only the date from the timestamp.
  "to_date(input_date) as input_date" # ensures input_date is recognized as a date type.
)

display(df_custom_expr)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4) spark sql

# COMMAND ----------

# MAGIC %md
# MAGIC **a) Convert String to Date (Default Format)**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT to_date('2025-03-29') AS converted_date;

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Convert String with Custom Format**
# MAGIC - If the **date** is in a **different format**, you can specify the **format** using **to_date()**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT to_date('29-03-2025', 'dd-MM-yyyy') AS converted_date;

# COMMAND ----------

spark.sql("select to_date('02-03-2013','MM-dd-yyyy') date").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **c) Convert Timestamp to Date**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT to_date('2025-03-29 14:30:00') AS converted_date;

# COMMAND ----------

# SQL TimestampType to DateType
spark.sql("select to_date(current_timestamp) as date_type").show()

# COMMAND ----------

# MAGIC %md
# MAGIC - **current_timestamp Function**
# MAGIC
# MAGIC   - This function returns the current timestamp (**including date and time**).
# MAGIC   - Example output: **2025-03-29 14:30:45.123**
# MAGIC
# MAGIC - **to_date(current_timestamp) Function**
# MAGIC
# MAGIC   - The **to_date()** function **extracts** only the **date part** from the **timestamp**.
# MAGIC   - It **removes** the **time portion** and **returns** only the **YYYY-MM-DD** format.
# MAGIC   - Example output: **2025-03-29**

# COMMAND ----------

# SQL CAST "TimestampType string" to "DateType"
spark.sql("select date(to_timestamp('2019-06-24 12:01:19.000')) as date_type").display()

# COMMAND ----------

# MAGIC %md
# MAGIC - **to_timestamp('2019-06-24 12:01:19.000')**
# MAGIC
# MAGIC   - This function **converts** the input **string '2019-06-24 12:01:19.000'** into a **TimestampType** value.
# MAGIC   - Output: 2019-06-24 12:01:19.000 (**TimestampType**)
# MAGIC
# MAGIC - **date(to_timestamp(...))**
# MAGIC
# MAGIC   - The **date()** function **extracts** only the **date** portion **(YYYY-MM-DD)** from the **TimestampType** value.
# MAGIC   - It effectively **truncates** the **time part** and **converts** it into a **DateType** value.
# MAGIC   - Output: 2019-06-24 (**DateType**)

# COMMAND ----------

# SQL CAST "timestamp string" to "DateType"
spark.sql("select date('2019-06-24 12:01:19.000') as date_type").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **'2019-06-24 12:01:19.000' (String Input)**
# MAGIC   - The input is a **timestamp** in **string** format **(YYYY-MM-DD HH:MI:SS.SSS)**.
# MAGIC
# MAGIC **date('2019-06-24 12:01:19.000')**
# MAGIC   - The **date()** function implicitly **converts** the **timestamp string** into a **DateType** by **extracting** only the **date** portion **(YYYY-MM-DD)**.
# MAGIC   - The **time** part (12:01:19.000) is **discarded**.
# MAGIC   - **Equivalent** to **CAST('2019-06-24 12:01:19.000' AS DATE)**.

# COMMAND ----------

# MAGIC %md
# MAGIC       # SQL Timestamp String (default format) to DateType
# MAGIC       spark.sql("select to_date('2019-06-24 12:01:19.000') as date_type").show()
# MAGIC                                          (or)
# MAGIC       spark.sql("select CAST('2019-06-24 12:01:19.000' AS DATE) as date_type").show()

# COMMAND ----------

# SQL Timestamp String (default format) to DateType
spark.sql("select to_date('2019-06-24 12:01:19.000') as date_type").display()

# COMMAND ----------

# MAGIC %md
# MAGIC - Input String: **'2019-06-24 12:01:19.000'**
# MAGIC   - This is a **timestamp string** in the default format: **YYYY-MM-DD HH:MI:SS.SSS**.
# MAGIC
# MAGIC - **to_date('2019-06-24 12:01:19.000') Function**
# MAGIC   - The **to_date()** function **extracts** only the **date (YYYY-MM-DD) part** from the input.
# MAGIC   - The **time** portion (**12:01:19.000**) is **discarded**.
# MAGIC   - Implicitly **converts** the **string to DateType**.

# COMMAND ----------

# MAGIC %md
# MAGIC       # SQL Custom Timeformat to DateType
# MAGIC       spark.sql("select to_date('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as date_type").show()
# MAGIC                                          (or)
# MAGIC       spark.sql("select CAST('2019-06-24 12:01:19.000' AS DATE) as date_type").show()

# COMMAND ----------

# SQL Custom Timeformat to DateType
spark.sql("select to_date('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as date_type").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Input String: '06-24-2019 12:01:19.000'**
# MAGIC   - The **date and time** are provided in a **custom format**: MM-dd-yyyy HH:mm:ss.SSSS
# MAGIC   - **"06-24-2019"** represents **June 24, 2019**.
# MAGIC   - **"12:01:19.000"** is the **time** portion, which will be **ignored**.
# MAGIC
# MAGIC **to_date('06-24-2019 12:01:19.000', 'MM-dd-yyyy HH:mm:ss.SSSS') Function**
# MAGIC   - **First argument:** The timestamp string to convert.
# MAGIC   - **Second argument:** The format of the input string.
# MAGIC   - The function **parses the string according to the provided format** and extracts only the **date part**.
# MAGIC   - The **time** portion (12:01:19.000) is **discarded**.
# MAGIC   - The result is converted to **DateType (YYYY-MM-DD)**.

# COMMAND ----------

# MAGIC %md
# MAGIC **d) Use to_date() on a Column**
# MAGIC - If you have a table with a **string** column containing **dates**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE EmpOrder (order_id INT, order_date STRING);
# MAGIC INSERT INTO EmpOrder VALUES (1, '29-03-2025'), (2, '30-03-2025');
# MAGIC
# MAGIC SELECT order_id, order_date, to_date(order_date, 'dd-MM-yyyy') AS formatted_date FROM EmpOrder;