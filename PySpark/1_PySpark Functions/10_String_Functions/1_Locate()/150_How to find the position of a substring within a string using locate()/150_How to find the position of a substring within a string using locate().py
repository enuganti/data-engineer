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

from pyspark.sql.functions import substring, concat, lit, col, expr, locate, when

# COMMAND ----------

# MAGIC %md
# MAGIC      df1 = df.withColumn("loc", locate(";", col("RecurrencePattern")))
# MAGIC - which **searches** from the **beginning of the string** (i.e., **position 1**).
# MAGIC
# MAGIC - This is **equivalent** to **locate(";", col("RecurrencePattern"), 1)**.

# COMMAND ----------

# MAGIC %md
# MAGIC      df1 = df.withColumn("loc", locate(";", col("RecurrencePattern"), 1)) 
# MAGIC - This explicitly sets the **start position to 1**, which is also the **beginning of the string**.

# COMMAND ----------

# MAGIC %md
# MAGIC      locate(";", col("RecurrencePattern"), 5)
# MAGIC - This would start **searching** from the **5th character**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Find Position of Substring**
# MAGIC - In this case, if **"data"** is found, the **position** will show the **index** of its **first occurrence**.

# COMMAND ----------

# DBTITLE 1,Find position of a substring
from pyspark.sql.functions import locate

# Sample data
data = [("Azure data engineer (ADE)", "suman@gmail.com"),
        ("AWS data engineer (AWS)", "kiranrathod@gmail.com"),
        ("data warehouse", "rameshwaran@gmail.com"),
        ("GCP engineer", "krishnamurthy@gmail.com"),
        ("PySpark engineer", "vishweswarrao@gmail.com")]

columns = ["text", "mail"]

df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

# Use locate() to find the position of 'data'
df_pos = df.withColumn("position", locate("data", df["text"]))

# Show the DataFrame with the position column
display(df_pos)

# COMMAND ----------

# DBTITLE 1,Filtering Rows Based on Substring Existence
# Filter rows where 'data' is found in 'text'
df_filtered = df \
    .withColumn("position", locate("data", df["text"])) \
    .filter(locate("data", col("text")) > 0)
display(df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Using locate to Find Substring in a Specific Column**
# MAGIC - This finds the **position** of the **"@"** symbol in the **email** column.
# MAGIC - The result will be the **index** position where **"@"** appears in **each email string**.

# COMMAND ----------

df_email = df.withColumn("position_of_email", locate("@", col("mail")))
display(df_email)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Finding Position of Substring in a Column with Multiple Occurrences**
# MAGIC
# MAGIC - If you have a **string** with **multiple occurrences** of the **substring** and want to know the **position** of the **first occurrence**.

# COMMAND ----------

from pyspark.sql.functions import locate

# Sample data
data = [("Azure data engineer data world", "suman@gmail.com"),
        ("AWS data engineer data type", "kiranrathod@gmail.com"),
        ("data warehouse data storage", "rameshwaran@gmail.com"),
        ("GCP engineer", "krishnamurthy@gmail.com"),
        ("PySpark engineer", "vishweswarrao@gmail.com")]

columns = ["text", "mail"]

dff = spark.createDataFrame(data, columns)
display(dff)

# COMMAND ----------

df_mltpl = dff.withColumn("position", locate("data", col("text")))
display(df_mltpl)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) Handling Missing Substrings with locate**
# MAGIC
# MAGIC - When the **substring doesn’t exist** in the string, locate() will return **0**.

# COMMAND ----------

df_miss = df.withColumn("position", locate("banana", col("mail")))
display(df_miss)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **5) Case Sensitivity in locate**
# MAGIC
# MAGIC - locate() is **case-sensitive**, so make sure to account for this.

# COMMAND ----------

# Only finds "data", not "Data"
df_sens = df.withColumn("position", locate("data", col("text"))) \
            .withColumn("pos", locate("Data", col("text")))
display(df_sens)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **6) Check If Substring Exists (Using locate with when and col)**
# MAGIC - Here, **data_present** will be **True** if the substring **data** is found and **False** otherwise.

# COMMAND ----------

# DBTITLE 1,Combining with Other Functions
from pyspark.sql.functions import when

# Create a new column "data_present" that returns True if 'data' is found, otherwise False.
df_with_conditional = df \
    .withColumn("location", locate("data", col("text"))) \
    .withColumn("data_present", when(locate("data", col("text")) > 0, True).otherwise(False))
display(df_with_conditional)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **7) Extract the Substring Position Using locate and substring**

# COMMAND ----------

# Use expr to apply substring and locate together
df_pos = df \
    .withColumn("location", locate("data", col("text"))) \
    .withColumn("data_substring", expr("substring(text, locate('data', text), 13)"))
display(df_pos)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **8) Replace Substring After Locating It**

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

df_rpl = df \
    .withColumn("location", locate("data", col("text"))) \
    .withColumn("updated_description",  
                when(locate("data", col("text")) > 0, regexp_replace(col("text"), "data", "Data"))
                .otherwise(col("text")))
display(df_rpl)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **9) Multiple Substring Search Using locate()**

# COMMAND ----------

# Find positions of both 'apple' and 'banana' in 'description' column
df_mltpl_str = df_rpl \
       .withColumn("data_position", locate("Data", col("updated_description"))) \
       .withColumn("text_position", locate("engineer", col("text"))) \
       .select("text", "text_position", "mail", "updated_description", "data_position")

display(df_mltpl_str)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **10) Using locate() with selectExpr**

# COMMAND ----------

# DBTITLE 1,selectExpr
df_new = df.selectExpr(
    "text",
    "locate('data', text) as data_position"
)

display(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **11) Using locate() with expr**

# COMMAND ----------

# DBTITLE 1,Use in SQL Expression with expr()
# Use expr to create a column with locate expression
df_expr = df.withColumn("position_expr", expr("locate('data', text)"))
display(df_expr)