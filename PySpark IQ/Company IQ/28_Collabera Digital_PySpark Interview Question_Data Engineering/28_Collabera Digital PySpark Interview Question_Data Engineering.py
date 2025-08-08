# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Question
# MAGIC **How to convert custom date into date format?**

# COMMAND ----------

data = [("0240312", "0231225", "0221120"),
        ("0231225", "0211225", "0251225"),
        ("0980312", "0991225", "0971225"),
        ("0961225", "0951225", "0940921"),
        ("0240312", "0231225", "0970618"),
        ("0850911", "0880713", "0820219"),
        ("0", "0991225", "0221120")
       ]
columns = ["d1", "d2", "d3"]

df_samp = spark.createDataFrame(data, columns)
display(df_samp)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Using substring and concat (Manual Parsing)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, concat, concat_ws, to_date, lpad

# COMMAND ----------

df_substr = df_samp.withColumn("parsed_date", to_date(
    concat_ws("-", 
        concat(lit("20"), col("d1").substr(2, 2)),  # Year
        col("d1").substr(4, 2),                     # Month
        col("d1").substr(6, 2)                      # Day
    ), "yyyy-MM-dd"
))

display(df_substr)

# COMMAND ----------

# MAGIC %md
# MAGIC |             |   |   |   |   |   |   |   |
# MAGIC |-------------|---|---|---|---|---|---|---|
# MAGIC | INDEX       | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
# MAGIC |String_date  | 0 | 2 | 4 | 0 | 3 | 1 | 2 |
# MAGIC |substr(2, 2) |   | 1 | 2 |   |   |   |   |
# MAGIC |substr(4, 2) |   |   |   | 1 | 2 |   |   |
# MAGIC |substr(6, 2) |   |   |   |   |   | 1 | 2 |

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Using unix_timestamp

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, unix_timestamp, from_unixtime

df1_ut = df_samp.withColumn("date_str", concat(lit("20"), col("d1").substr(2, 2), col("d1").substr(4, 2), col("d1").substr(6, 2))) \
                .withColumn("parsed_date_ut", unix_timestamp(col("date_str"), "yyyyMMdd")) \
                .withColumn("parsed_date_fu_ut", from_unixtime(unix_timestamp(col("date_str"), "yyyyMMdd")).cast("date")) \
                .withColumn("parsed_date_fu_ut", from_unixtime(unix_timestamp(col("date_str"), "yyyyMMdd")).cast("timestamp"))

display(df1_ut)

# COMMAND ----------

# MAGIC %md
# MAGIC **from_unixtime:**
# MAGIC
# MAGIC - Converting **Unix Time** to a **Human-Readable Format** of timestamp.
# MAGIC
# MAGIC | unix_time |      timestamp          |
# MAGIC |-----------|-------------------------|
# MAGIC | 1648974310  |  2023-04-03 09:45:10  |
# MAGIC
# MAGIC - **Returns:** string of **default: yyyy-MM-dd HH:mm:ss**

# COMMAND ----------

# MAGIC %md
# MAGIC **unix_timestamp:**
# MAGIC
# MAGIC | string_date  |    unix_timestamp  |
# MAGIC |--------------|--------------------|
# MAGIC | 20140228     |   1393545600       |

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Using to_date with format directly
# MAGIC - This method works best when you're sure the column only contains **valid date-like strings**.
# MAGIC - **Non-date values (like "0")** will be returned as **null**.

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, to_date, when

df_todate = df_samp.withColumn("date_str", when(col("d1") != "0", concat(lit("20"), col("d1").substr(2, 2), col("d1").substr(4, 2), col("d1").substr(6, 2)))) \
                   .withColumn("parsed_date", to_date(col("date_str"), "yyyyMMdd"))

display(df_todate)

# COMMAND ----------

# MAGIC %md
# MAGIC **col(col_name) != "0"**
# MAGIC
# MAGIC - This checks **if the value** in column **col_name** is **not equal to "0"**.
# MAGIC - This condition ensures that the transformation is applied **only to rows** where the **column does not contain "0"**.
# MAGIC
# MAGIC **When the condition is True (col(col_name) != "0"):**
# MAGIC
# MAGIC - **substr(2, len(col_name)-1)** extracts a substring from the **2nd character** onward.
# MAGIC - **to_date(..., 'yyMMdd')** converts the extracted substring into a proper date format **(yyMMdd)**.
# MAGIC
# MAGIC **Otherwise (col(col_name) == "0"):**
# MAGIC
# MAGIC - The original value **(col(col_name))** is retained without modification.
# MAGIC
# MAGIC **.cast("date"):**
# MAGIC
# MAGIC - The entire column is **cast** to **date type**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Processing Step-by-Step:**
# MAGIC
# MAGIC - For **"0240312"** (Not "0") => Extract **240312** => Convert to **2024-03-12**.
# MAGIC
# MAGIC - For **"0"** (Matches "0") => Keep **"0"** as is.
# MAGIC
# MAGIC **Final Output (df):**
# MAGIC      
# MAGIC      col_name
# MAGIC      2024-03-12
# MAGIC      2023-12-25
# MAGIC      0

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4) Handling Non-Date Values (e.g., "0") Safely

# COMMAND ----------

from pyspark.sql.functions import when, col, lit, concat, to_date

df_nondate = df_samp.withColumn(
    "parsed_date",
    when(
        col("d1").rlike("^[0-9]{7}$"),  # Only 7-digit numbers
        to_date(
            concat(lit("20"), col("d1").substr(2, 2), col("d1").substr(4, 2), col("d1").substr(6, 2)),
            "yyyyMMdd"
        )
    ).otherwise(None)
)

display(df_nondate)

# COMMAND ----------

# MAGIC %md
# MAGIC **Regex check**
# MAGIC - **^[0-9]{7}$** → ensures the value is **exactly 7 digits** (so **"0" will be skipped**).
# MAGIC
# MAGIC **String transformation**
# MAGIC - col("d1").substr(2, 2) → year (last two digits)
# MAGIC - col("d1").substr(4, 2) → month
# MAGIC - col("d1").substr(6, 2) → day
# MAGIC - Then prepend **"20"** to get a proper **yyyyMMdd**.
# MAGIC
# MAGIC **to_date(..., "yyyyMMdd")**
# MAGIC - Converts the new string to a real date type.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5) to_date & substr

# COMMAND ----------

from pyspark.sql import functions as F

# function to convert the date fields into required format
def convert_date_fields(df, col_names):
    for col_name in col_names:
        df = df.withColumn(
            col_name,
            F.when(
                (F.col(col_name) != "0") & (F.col(col_name).rlike("^[0-9]{7}$")),
                F.to_date(
                    F.concat(
                        F.lit("20"),
                        F.col(col_name).substr(2, 2),
                        F.col(col_name).substr(4, 2),
                        F.col(col_name).substr(6, 2)
                    ),
                    "yyyyMMdd"
                )
            ).otherwise(F.lit(None).cast("date"))
        )
    return df

# COMMAND ----------

q = convert_date_fields(df_samp, columns)
display(q)

# COMMAND ----------

# MAGIC %md
# MAGIC - Regex check to ensure only 7-digit numbers are processed.
# MAGIC - Proper substr() slicing:
# MAGIC   - .substr(2, 2) → **last two digits of year**
# MAGIC   - .substr(4, 2) → **month**
# MAGIC   - .substr(6, 2) → **day**
# MAGIC - concat() with **"20"** to make a **yyyyMMdd** string.
# MAGIC - otherwise() returns a real date-typed **null** for **invalid entries**.