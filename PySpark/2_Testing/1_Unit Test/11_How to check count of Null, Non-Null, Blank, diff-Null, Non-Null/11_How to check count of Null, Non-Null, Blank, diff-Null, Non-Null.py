# Databricks notebook source
# MAGIC %md
# MAGIC #### Unit Test
# MAGIC
# MAGIC **How to count Null's, Non-Null's, Blank, Non-Blank values**
# MAGIC
# MAGIC      a) Count of NULL values
# MAGIC      b) Count of non-NULL values
# MAGIC      c) Count of blank (empty string) values
# MAGIC      d) Count of non-null and non-blank values
# MAGIC      e) Check for any of the specified columns is null's
# MAGIC      f) How to find the number of rows where all columns are NULL?
# MAGIC      g) How to count the number of NULL values in a column?
# MAGIC      h) All in one summary
# MAGIC      i) All in one summary using SUM(CASE … END)
# MAGIC      j) Grouping by status

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("OBJECT_ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("Customer_ID", IntegerType(), True),
    StructField("Change_Date", StringType(), True),
    StructField("Load_Date", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("description", StringType(), True),
    StructField("start_date_source", StringType(), True),
    StructField("start_date_bronze", StringType(), True)
])


# COMMAND ----------

# DBTITLE 1,1) sample dataframe
data = [(583069, "Harish", None, 13681832, None, '2025-06-02', None, 'E-Mail', 1724256609000, None),
        (510102, "", "HR", 40685884, '2025-04-02T04:15:05Z', '2025-06-02', 'Finished', 'Internet', 1724256609000, None),
        (506654, "Basha", "", None, '2025-04-02T04:15:05Z', '2025-06-02', 'Not Relevant', 'Social Media', 1724256609000, None),
        (583195, None, "Finance", 12619703, None, '2025-06-02', 'Started', 'Messaging', 1724256609000, None),
        (470450, "Venky", "IT", 8541938, '2025-04-02T07:59:14Z', '2025-06-02', 'Not Relevant', 'IoT', 1724256609000, None),
        (558253, "", None, 2269299, None, '2025-06-02', 'Open', None, 1724256609000, None),
        (None, "Krishna", "Sales", None, '2025-04-02T06:12:18Z', '2025-06-02', None, 'Manual data entry', 1724256609000, None),
        (583181, "Kiran", "Marketing", 39714449, None, '2025-06-02', 'Finished', 'Other', 1724256609000, None),
        (583119, "Hitesh", None, 10183510, '2025-04-02T04:15:13Z', None, 'Open', 'Telephony', 1724256609000, None),
        (577519, "", "Accounts", None, '2025-04-02T08:27:50Z', '2025-06-02', 'Not Relevant', None, 1724256609000, None),
        (583151, "Sushma", "Accounts", 40442877, None, '2025-06-02', 'Open', 'Fax', 1724256609000, None),
        (583167, None, "Admin", 16474490, '2025-04-02T09:07:27Z', None, 'Not Relevant', 'Feedback', 1724256609000, None),
        (583162, "Buvan", "IT", 7447339, '2025-04-02T16:46:07Z', None, 'Finished', 'WorkZone', 1724256609000, None),
        (575216, "Mohan", "Admin", 17258071, '2025-04-02T01:51:03Z', '2025-06-02', 'Open', 'IOT', 1724256609000, None),
        (None, None, None, None, None, None, None, None, None, None),
        (583173, "Lohith", "Finance", 15113750, None, '2025-06-02', 'Finished', None, 1724256609000, None),
        (583099, "Loba", "Testing", 40505376, '2025-04-02T19:54:50Z', None, 'Started', None, 1724256609000, None)
       ]

df_dev = spark.createDataFrame(data, schema)
display(df_dev)

# COMMAND ----------

# DBTITLE 1,2) write to delta
# df_dev.write \
#       .format("delta") \
#       .mode("overwrite") \
#       .option("path", "/user/hive/warehouse/bronze_Nulls") \
#       .saveAsTable("tbl_NonNull_Nulls_Blank")

df_dev.createOrReplaceTempView("tbl_NonNull_Nulls_Blank")

# COMMAND ----------

# MAGIC %md
# MAGIC #### a) PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC **Null_Count:** Number of actual nulls
# MAGIC
# MAGIC **NotNull_Count:** includes both blanks and real values
# MAGIC
# MAGIC **Blank_Count:** number of empty strings ("")
# MAGIC
# MAGIC **NonNull_And_Not_Blank** = NotNull_Count - Blank_Count

# COMMAND ----------

# DBTITLE 1,1) Function: Non-Null, Null & Blank
from pyspark.sql.functions import lit, count, when, col

def count_values(df, column):
    return df_dev.select(
        count(when(col(column).isNull(), column)).alias("Null_Count"),
        count(when(col(column).isNotNull(), column)).alias("NotNull_Count"),
        count(when(col(column) == "", column)).alias("Blank_Count"),
        count(lit(1)).alias("Total_Rows")
        )

# COMMAND ----------

# DBTITLE 1,2) Result: Non-Null, Null & Blank
display(count_values(df_dev, "Name"))
display(count_values(df_dev, "department"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### b) SQL

# COMMAND ----------

# MAGIC %md
# MAGIC **a) How to check for NULL values in a column?**
# MAGIC - Use the `IS NULL` or `IS NOT NULL` condition in the WHERE clause.

# COMMAND ----------

# DBTITLE 1,1) Null's: Primary Key
# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC WHERE OBJECT_ID IS NULL;

# COMMAND ----------

# DBTITLE 1,2) List all Null values of Primary Key
# MAGIC %sql
# MAGIC SELECT * FROM tbl_NonNull_Nulls_Blank
# MAGIC WHERE OBJECT_ID IS NULL;

# COMMAND ----------

# DBTITLE 1,3) Count NULLs
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS null_count
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC WHERE Name IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC **b) How to check for Non-NULL values in a column?**

# COMMAND ----------

# DBTITLE 1,4) Count Non-NULL values
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS non_null_count
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC WHERE Name IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC - COUNT(Name) **ignores NULLs**, so this gives count of **non-NULL** values.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(Name) AS non_null_count
# MAGIC FROM tbl_NonNull_Nulls_Blank;

# COMMAND ----------

# MAGIC %md
# MAGIC **c) Count of blank (empty string) values**
# MAGIC
# MAGIC **Note:**
# MAGIC - This applies to **string/text** columns.
# MAGIC - Also, in some databases (like **Oracle**), **empty strings** are treated as **NULL**, so this may not apply the same way.

# COMMAND ----------

# DBTITLE 1,5) Count Blank (empty string) values
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS blank_count
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC WHERE Name = '';

# COMMAND ----------

# MAGIC %md
# MAGIC **d) Count of non-null and non-blank values**

# COMMAND ----------

# DBTITLE 1,6) Count of non-null and non-blank values
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS non_null_non_blank_count
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC WHERE Name IS NOT NULL AND Name <> '';

# COMMAND ----------

# MAGIC %md
# MAGIC **e) Check for any of the specified columns is null's**
# MAGIC - This query returns all rows from the table non_null_non_blank_count where **any of the specified columns** (id, name, department, or Age) have a **NULL** value.
# MAGIC
# MAGIC - The **OR** operators mean that if **even one of these columns** is **NULL** in a row, that row will be selected.

# COMMAND ----------

# DBTITLE 1,7) Check for any of the specified columns is null's
# MAGIC %sql
# MAGIC SELECT * FROM tbl_NonNull_Nulls_Blank
# MAGIC WHERE OBJECT_ID IS NULL OR
# MAGIC       Name IS NULL OR
# MAGIC       department IS NULL OR
# MAGIC       Customer_ID IS NULL OR
# MAGIC       Change_Date IS NULL OR
# MAGIC       Load_Date IS NULL OR
# MAGIC       Status IS NULL OR
# MAGIC       description IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC **f) How to find the number of rows where all columns are NULL?**

# COMMAND ----------

# DBTITLE 1,8) Find the number of rows where all columns are NULL
# MAGIC %sql
# MAGIC SELECT *
# MAGIC -- SELECT COUNT(*) AS all_null_rows
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC WHERE OBJECT_ID IS NULL AND
# MAGIC       Name IS NULL AND
# MAGIC       department IS NULL AND
# MAGIC       Customer_ID IS NULL AND
# MAGIC       Change_Date IS NULL AND
# MAGIC       Load_Date IS NULL AND
# MAGIC       Status IS NULL AND
# MAGIC       description IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC **g) How to count the number of NULL values in a column?**

# COMMAND ----------

# DBTITLE 1,9) How to count the number of NULL values in a column?
# MAGIC %sql
# MAGIC SELECT COUNT(CASE WHEN description IS NULL THEN 1 END) AS NullCount_description
# MAGIC FROM tbl_NonNull_Nulls_Blank;

# COMMAND ----------

# MAGIC %md
# MAGIC **h) All in one summary**

# COMMAND ----------

# MAGIC %md
# MAGIC **COUNT(*):**
# MAGIC - Counts all rows.
# MAGIC
# MAGIC **your_column IS NULL:**
# MAGIC - Checks for NULL values.
# MAGIC
# MAGIC **your_column = '':**
# MAGIC - Checks for **blank/empty strings** (only relevant for text/varchar fields).
# MAGIC
# MAGIC **your_column IS NOT NULL AND your_column <> '':**
# MAGIC - Counts values that are **not NULL and not blank**.

# COMMAND ----------

# DBTITLE 1,10) name: Non-Null, Null & Blank
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) AS Total_Count,
# MAGIC     COUNT(CASE WHEN Name IS NULL THEN 1 END) AS Null_Count,
# MAGIC     COUNT(CASE WHEN Name IS NOT NULL THEN 1 END) AS Non_Null_Count,
# MAGIC     COUNT(CASE WHEN Name = '' THEN 1 END) AS Blank_Count,
# MAGIC     COUNT(CASE WHEN Name IS NOT NULL AND Name <> '' THEN 1 END) AS valid_non_blank_count,
# MAGIC     (COUNT(CASE WHEN Name IS NOT NULL THEN 1 END) - 
# MAGIC      COUNT(CASE WHEN Name = '' THEN 1 END)) AS Blank_Count_Difference,
# MAGIC     (COUNT(CASE WHEN Name IS NOT NULL THEN 1 END) - 
# MAGIC      COUNT(CASE WHEN Name IS NULL THEN 1 END)) AS Null_Count_Difference
# MAGIC FROM tbl_NonNull_Nulls_Blank;

# COMMAND ----------

# MAGIC %md
# MAGIC **Expression A:**
# MAGIC
# MAGIC      COUNT(CASE WHEN name IS NOT NULL AND name <> '' THEN 1 END) AS valid_non_blank_count
# MAGIC
# MAGIC This counts all rows where:
# MAGIC - **name** is **not NULL**, and
# MAGIC - **name** is **not a blank string ('')**.
# MAGIC
# MAGIC ✅ Not NULL and Not blank → **"valid non-blank values"**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Expression B:**
# MAGIC
# MAGIC      (COUNT(CASE WHEN name IS NOT NULL THEN 1 END) - COUNT(CASE WHEN name = '' THEN 1 END)) AS Blank_Count_Difference
# MAGIC
# MAGIC - Counts all **non-NULL** values: **COUNT(CASE WHEN name IS NOT NULL THEN 1 END)**
# MAGIC - Then subtracts the **number of blank strings** (regardless of NULL status): **COUNT(CASE WHEN name = '' THEN 1 END)**
# MAGIC

# COMMAND ----------

# DBTITLE 1,11) department: Non-Null, Null & Blank
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) AS Total_Count,
# MAGIC     COUNT(CASE WHEN department IS NULL THEN 1 END) AS Null_Count,
# MAGIC     COUNT(CASE WHEN department IS NOT NULL THEN 1 END) AS Non_Null_Count,
# MAGIC     COUNT(CASE WHEN department = '' THEN 1 END) AS Blank_Count,
# MAGIC     COUNT(CASE WHEN department IS NOT NULL AND department <> '' THEN 1 END) AS valid_non_blank_count,
# MAGIC     (COUNT(CASE WHEN department IS NOT NULL THEN 1 END) - 
# MAGIC      COUNT(CASE WHEN department = '' THEN 1 END)) AS Blank_Count_Difference,
# MAGIC     (COUNT(CASE WHEN department IS NOT NULL THEN 1 END) - 
# MAGIC      COUNT(CASE WHEN department IS NULL THEN 1 END)) AS Null_Count_Difference
# MAGIC FROM tbl_NonNull_Nulls_Blank;

# COMMAND ----------

# DBTITLE 1,12) All Columns in Tabular Format
# MAGIC %sql
# MAGIC SELECT 'OBJECT_ID' AS Column_Name,
# MAGIC     COUNT(*) AS Total_Count,
# MAGIC     COUNT(CASE WHEN OBJECT_ID IS NULL THEN 1 END) AS Null_Count,
# MAGIC     COUNT(CASE WHEN OBJECT_ID IS NOT NULL THEN 1 END) AS Non_Null_Count,
# MAGIC     NULL AS Blank_Count,
# MAGIC     COUNT(CASE WHEN OBJECT_ID IS NOT NULL THEN 1 END) AS Valid_Non_Blank_Count,
# MAGIC     NULL AS Blank_Count_Difference,
# MAGIC     (COUNT(CASE WHEN OBJECT_ID IS NOT NULL THEN 1 END) - COUNT(CASE WHEN OBJECT_ID IS NULL THEN 1 END)) AS Null_Count_Difference
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Name',
# MAGIC     COUNT(*),
# MAGIC     COUNT(CASE WHEN Name IS NULL THEN 1 END),
# MAGIC     COUNT(CASE WHEN Name IS NOT NULL THEN 1 END),
# MAGIC     COUNT(CASE WHEN Name = '' THEN 1 END),
# MAGIC     COUNT(CASE WHEN Name IS NOT NULL AND Name <> '' THEN 1 END),
# MAGIC     (COUNT(CASE WHEN Name IS NOT NULL THEN 1 END) - COUNT(CASE WHEN Name = '' THEN 1 END)),
# MAGIC     (COUNT(CASE WHEN Name IS NOT NULL THEN 1 END) - COUNT(CASE WHEN Name IS NULL THEN 1 END))
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'department',
# MAGIC     COUNT(*),
# MAGIC     COUNT(CASE WHEN department IS NULL THEN 1 END),
# MAGIC     COUNT(CASE WHEN department IS NOT NULL THEN 1 END),
# MAGIC     COUNT(CASE WHEN department = '' THEN 1 END),
# MAGIC     COUNT(CASE WHEN department IS NOT NULL AND department <> '' THEN 1 END),
# MAGIC     (COUNT(CASE WHEN department IS NOT NULL THEN 1 END) - COUNT(CASE WHEN department = '' THEN 1 END)),
# MAGIC     (COUNT(CASE WHEN department IS NOT NULL THEN 1 END) - COUNT(CASE WHEN department IS NULL THEN 1 END))
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Customer_ID',
# MAGIC     COUNT(*) AS Total_Count,
# MAGIC     COUNT(CASE WHEN Customer_ID IS NULL THEN 1 END) AS Null_Count,
# MAGIC     COUNT(CASE WHEN Customer_ID IS NOT NULL THEN 1 END) AS Non_Null_Count,
# MAGIC     NULL AS Blank_Count,
# MAGIC     COUNT(CASE WHEN Customer_ID IS NOT NULL THEN 1 END) AS Valid_Non_Blank_Count,
# MAGIC     NULL AS Blank_Count_Difference,
# MAGIC     (COUNT(CASE WHEN Customer_ID IS NOT NULL THEN 1 END) - COUNT(CASE WHEN Customer_ID IS NULL THEN 1 END)) AS Null_Count_Difference
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Change_Date',
# MAGIC     COUNT(*),
# MAGIC     COUNT(CASE WHEN Change_Date IS NULL THEN 1 END),
# MAGIC     COUNT(CASE WHEN Change_Date IS NOT NULL THEN 1 END),
# MAGIC     COUNT(CASE WHEN Change_Date = '' THEN 1 END),
# MAGIC     COUNT(CASE WHEN Change_Date IS NOT NULL AND Change_Date <> '' THEN 1 END),
# MAGIC     (COUNT(CASE WHEN Change_Date IS NOT NULL THEN 1 END) - COUNT(CASE WHEN Change_Date = '' THEN 1 END)),
# MAGIC     (COUNT(CASE WHEN Change_Date IS NOT NULL THEN 1 END) - COUNT(CASE WHEN Change_Date IS NULL THEN 1 END))
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Load_Date',
# MAGIC     COUNT(*),
# MAGIC     COUNT(CASE WHEN Load_Date IS NULL THEN 1 END),
# MAGIC     COUNT(CASE WHEN Load_Date IS NOT NULL THEN 1 END),
# MAGIC     COUNT(CASE WHEN Load_Date = '' THEN 1 END),
# MAGIC     COUNT(CASE WHEN Load_Date IS NOT NULL AND Load_Date <> '' THEN 1 END),
# MAGIC     (COUNT(CASE WHEN Load_Date IS NOT NULL THEN 1 END) - COUNT(CASE WHEN Load_Date = '' THEN 1 END)),
# MAGIC     (COUNT(CASE WHEN Load_Date IS NOT NULL THEN 1 END) - COUNT(CASE WHEN Load_Date IS NULL THEN 1 END))
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Status',
# MAGIC     COUNT(*),
# MAGIC     COUNT(CASE WHEN Status IS NULL THEN 1 END),
# MAGIC     COUNT(CASE WHEN Status IS NOT NULL THEN 1 END),
# MAGIC     COUNT(CASE WHEN Status = '' THEN 1 END),
# MAGIC     COUNT(CASE WHEN Status IS NOT NULL AND Status <> '' THEN 1 END),
# MAGIC     (COUNT(CASE WHEN Status IS NOT NULL THEN 1 END) - COUNT(CASE WHEN Status = '' THEN 1 END)),
# MAGIC     (COUNT(CASE WHEN Status IS NOT NULL THEN 1 END) - COUNT(CASE WHEN Status IS NULL THEN 1 END))
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'description',
# MAGIC     COUNT(*),
# MAGIC     COUNT(CASE WHEN description IS NULL THEN 1 END),
# MAGIC     COUNT(CASE WHEN description IS NOT NULL THEN 1 END),
# MAGIC     COUNT(CASE WHEN description = '' THEN 1 END),
# MAGIC     COUNT(CASE WHEN description IS NOT NULL AND description <> '' THEN 1 END),
# MAGIC     (COUNT(CASE WHEN description IS NOT NULL THEN 1 END) - COUNT(CASE WHEN description = '' THEN 1 END)),
# MAGIC     (COUNT(CASE WHEN description IS NOT NULL THEN 1 END) - COUNT(CASE WHEN description IS NULL THEN 1 END))
# MAGIC FROM tbl_NonNull_Nulls_Blank;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **g) All in one summary using SUM(CASE … END)**

# COMMAND ----------

# DBTITLE 1,13) All in one summary using SUM(CASE … END)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN department IS NULL THEN 1 ELSE 0 END) AS num_null,
# MAGIC   SUM(CASE WHEN department = '' THEN 1 ELSE 0 END) AS num_blank,
# MAGIC   SUM(CASE WHEN department IS NOT NULL 
# MAGIC             AND department <> '' THEN 1 ELSE 0 END) AS num_non_null_non_blank
# MAGIC FROM tbl_NonNull_Nulls_Blank;

# COMMAND ----------

# MAGIC %md
# MAGIC **h) Grouping by status**

# COMMAND ----------

# DBTITLE 1,14) Grouping by status
# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN Name IS NULL THEN 'NULL'
# MAGIC     WHEN Name = '' THEN 'BLANK'
# MAGIC     ELSE 'NON-NULL/NON-BLANK'
# MAGIC   END AS status,
# MAGIC   COUNT(*) AS cnt
# MAGIC FROM tbl_NonNull_Nulls_Blank
# MAGIC GROUP BY
# MAGIC   CASE
# MAGIC     WHEN Name IS NULL THEN 'NULL'
# MAGIC     WHEN Name = '' THEN 'BLANK'
# MAGIC     ELSE 'NON-NULL/NON-BLANK'
# MAGIC   END;