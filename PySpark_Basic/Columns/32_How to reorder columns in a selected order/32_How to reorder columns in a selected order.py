# Databricks notebook source
# MAGIC %md
# MAGIC ##### How to reorder columns in a selected order?

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

data = [
    ("2025-09-01", "Blue", "google", "GCC", "Search", "mobile", 123, "SONY", "daily", "launch", "Sales", 100),
    ("2025-07-11", "Green", "facebook", "parts", "Social", "desktop", 456, "BRAVIA", "weekly", "production", "gether", 200),
    ("2024-06-21", "Yellow", "instagram", "AWS", "cloud", "tab", 123, "IBM", "monthly", "roll-out", "Marketing", 100),
    ("2023-04-15", "Dark", "gamil", "Cloude", "bing", "monitor", 456, "DELPHI", "yearly", "inaguaration", "Admin", 200),
    ("2022-03-17", "Brown", "redbus", "Azure", "service", "iphone", 123, "SOLAR", "daily", "close", "Finance", 100),
    ("2021-02-18", "Pink", "linkedin", "SQL", "rediff", "keyboars", 456, "TAFFE", "bi-weekly", "vacation", "Accounts", 200)
    ]

schema = ["Sales_DT", "Product_NM", "Source_NM", "Target_NM", "Product_Group", "Device_Category", "Product_ID", "Company_NM", "Grade", "Event_Name", "Event_Type", "Sessions_CNT"]

df_order = spark.createDataFrame(data, schema)
display(df_order)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**
# MAGIC - Use **select()** with **column list**

# COMMAND ----------

# Ordered column list
var_lst_ordered_clms = [
    "Sales_DT",
    "Product_NM",
    "Source_NM",
    "Target_NM",
    "Product_Group",
    "Device_Category",
    "Product_ID",
    "Company_NM",
    "Grade",
    "Event_Name",
    "Event_Type",
    "Sessions_CNT",
]

# Reorder DataFrame
df_reordered = df_order.select(var_lst_ordered_clms)

# Show result
display(df_reordered)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 02**
# MAGIC - Use **df[columns]**

# COMMAND ----------

new_order = ["Sales_DT", "Product_ID", "Product_NM", "Product_Group", "Source_NM", "Target_NM", "Device_Category", "Company_NM", "Grade", "Event_Name", "Event_Type", "Sessions_CNT"]

df_mt02 = df_order[new_order]
display(df_mt02)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 03**
# MAGIC - Use **selectExpr()**

# COMMAND ----------

df_sexpr = df_order.selectExpr("Sales_DT", "Product_ID as product_id", "Product_NM as product_name", "Product_Group", "Source_NM", "Target_NM", "Device_Category", "Company_NM as company_name", "Grade", "Event_Name", "Event_Type", "Sessions_CNT as sessions_count")
display(df_sexpr)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 04**

# COMMAND ----------

def select_ordered_cols(df, list_ordered_cols):
  return df.select(list_ordered_cols)

# COMMAND ----------

ordered_cols = ["Sales_DT", "Product_ID", "Product_NM", "Product_Group", "Source_NM", "Target_NM", "Device_Category", "Company_NM", "Grade", "Event_Name", "Event_Type", "Sessions_CNT"]

method_02 = select_ordered_cols(df_order, ordered_cols)
display(method_02)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 05**

# COMMAND ----------

dbutils.widgets.text("list_ordered_clms", "", "list_ordered_clms")
widget_value = dbutils.widgets.get("list_ordered_clms")
if widget_value.strip():
    list_ordered_clms = eval(widget_value)
else:
    list_ordered_clms = []

# COMMAND ----------

# MAGIC %md
# MAGIC - The **strip()** method in Python is used to **remove leading (starting)** and **trailing (ending) whitespaces** characters from a given **string**.
# MAGIC
# MAGIC **strip():** Trim whitespace from Strings using strip(), LSTRIP, RSTRIP -> python playlist

# COMMAND ----------

def select_ordered_cols(df, list_ordered_cols):
  return df.select(list_ordered_cols)

# COMMAND ----------

# ["Sales_DT", "Product_ID", "Product_NM", "Product_Group", "Source_NM", "Target_NM", "Device_Category", "Company_NM", "Grade", "Event_Name", "Event_Type", "Sessions_CNT"]

method_03 = select_ordered_cols(df_order, list_ordered_clms)
display(method_03)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 06**

# COMMAND ----------

# Move "Grade" column to the front
cols = df_order.columns
new_order = ["Grade"] + [c for c in cols if c != "Grade"]

df_reordered_new = df_order.select(new_order)
display(df_reordered_new)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 07**
# MAGIC - Use **withColumn + reorder** using select

# COMMAND ----------

df_add_col = df_order.withColumn("country", F.lit("India"))

cols_reordered = ["country"] + [c for c in df_order.columns]

df_final_ord = df_add_col.select(cols_reordered)
display(df_final_ord)