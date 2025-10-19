# Databricks notebook source
# MAGIC %md
# MAGIC ##### How to add columns using dictionary?

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import lit

# COMMAND ----------

df_ts = spark.read.csv("/Volumes/@azureadb/pyspark/timestamp/timestamptodate.csv", header=True, inferSchema=True)
display(df_ts.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Add columns from a dictionary of literal values**
# MAGIC - If your **dictionary** has **column names and constant values**.

# COMMAND ----------

# MAGIC %md
# MAGIC      # Method 01
# MAGIC      for col, val in col_dict.items():
# MAGIC          df = df.withColumn(col, F.lit(val))
# MAGIC
# MAGIC      # Method 02
# MAGIC      df_ts_adv = df_ts.withColumn("productivity", F.lit("advertisement")) \
# MAGIC                       .withColumn("Sales_ID", F.lit("NULL")) \
# MAGIC                       .withColumn("Sales_Name", F.lit("NULL")) \
# MAGIC                       .withColumn("Granularity", F.lit("product_category"))

# COMMAND ----------

def add_col(df, dic_name_value):
    for col_name, col_value in dic_name_value.items():
        df = df.withColumn(col_name, F.lit(col_value))
    return df

# COMMAND ----------

dic_add_cols = {"productivity": "advertisement", "Sales": "NULL", "Sales_Name": "NULL", "Granularity": "product_category", "country": "India", "status": "Active"}

df_ts_adv = add_col(df_ts, dic_add_cols)
display(df_ts_adv.limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Add columns from a dictionary of expressions**
# MAGIC - If your dictionary maps new column names to transformations

# COMMAND ----------

# Example DataFrame
df_exp = spark.createDataFrame([(1, "Roja", 3),
                                (2, "Bibin", 4),
                                (3, "Rajesh", 5),
                                (4, "Priya", 6),
                                (5, "Mohan", 9),],
                               ["id", "name", "sales"])

# Dictionary: new column names -> constant values
col_dict = {"country": "India", "status": "Active"}

df_dict = df_exp
for col, val in col_dict.items():
    df_dict = df_dict.withColumn(col, F.lit(val))

display(df_dict)

# COMMAND ----------

expr_dict = {
    "name_upper": F.upper(F.col("name")),
    "sales_squared": (F.col("id") ** 2),
    "id_plus_10": F.col("id") + 10,
    "constant_val": F.lit(50),
    "name_length": F.length(F.col("name"))
}

df_dict_expr = df_exp

for col, expr in expr_dict.items():
    df_dict_expr = df_dict_expr.withColumn(col, expr)

display(df_dict_expr)

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Add columns from a dictionary of different datatypes**

# COMMAND ----------

col_dict = {"is_valid": True, "score": 95.5}

df_dtypes = df_dict_expr
for col, val in col_dict.items():
    df_dtypes = df_dtypes.withColumn(col, F.lit(val))

display(df_dtypes)
df_dtypes.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Adding Multiple New Columns from a Dictionary**

# COMMAND ----------

# Dictionary containing new column names and their literal values
new_columns_dict = {"city": "New York", "country": "USA"}

# Add multiple columns using withColumns()
df_with_new_cols = df.withColumns(
    {col_name: lit(value) for col_name, value in new_columns_dict.items()}
)

display(df_with_new_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Using select + dictionary unpacking (one-shot)**

# COMMAND ----------

df_dict = df.select("*", *[F.lit(v).alias(k) for k, v in col_dict.items()])
display(df_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC       [F.lit(v).alias(k) for k, v in col_dict.items()]
# MAGIC
# MAGIC - Loops through each **key, value** pair in the dictionary **col_dict**.
# MAGIC - **F.lit(v)** → creates a literal column (constant value for every row).
# MAGIC - **.alias(k)** → names that new column with the dictionary key.

# COMMAND ----------

# MAGIC %md
# MAGIC      col_dict = {"is_valid": True, "score": 95.5}
# MAGIC      [F.lit("True").alias("is_valid"), F.lit(95.5).alias("score")]
# MAGIC      
# MAGIC      col_dict = {"country": "India", "status": "Active"}
# MAGIC      [F.lit("India").alias("country"), F.lit("Active").alias("status")]
# MAGIC
# MAGIC - The * before the **list**
# MAGIC   - **Unpacks the list** so each element is passed as a separate argument to select.
# MAGIC   - Equivalent to writing:
# MAGIC
# MAGIC         df.select("*", F.lit("India").alias("country"), F.lit("Active").alias("status"))