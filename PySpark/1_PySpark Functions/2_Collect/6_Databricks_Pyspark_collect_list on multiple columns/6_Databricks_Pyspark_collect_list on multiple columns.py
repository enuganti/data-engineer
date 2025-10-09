# Databricks notebook source
# MAGIC %md
# MAGIC #### **Multiple Columns: collect_list()**

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import lit, col, collect_list, collect_set

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/Sales_Collect.csv", header=True, inferSchema=True)
display(df.limit(10))
df.printSchema()
print("Number of Rows:", df.count())

# COMMAND ----------

print("Column Names:\n\n", df.columns)
print("\n Number of columns:", len(df.columns))

# COMMAND ----------

df.select("Vehicle_Id").distinct().show()

# COMMAND ----------

df1 = df.groupby("Vehicle_Id")\
        .agg(f.collect_list("Vehicle_Price_Id").alias('SALES_EXP'))
display(df1)

# COMMAND ----------

sales_input_cols = ["Vehicle_Price_Id", "Vehicle_Showroom_Price"]

df2 = df.groupby("Vehicle_Id")\
        .agg(f.collect_list(f.struct(sales_input_cols)).alias('SALES_EXPECT'))
display(df2)

# COMMAND ----------

sales_input_cols1 = ["Vehicle_Price_Id", "Vehicle_Showroom_Price", "Vehicle_Showroom_Delta", "Vehicle_Showroom_Payment_Date"]

df3 = df.groupby("Vehicle_Id")\
        .agg(f.collect_list(f.struct(sales_input_cols1)).alias('SALES_EXPECTATION'))
display(df3)

# COMMAND ----------

[col for col in df.columns]

# COMMAND ----------

sales_input_cols_03 = ["Vehicle_Price_Id", "Vehicle_Showroom_Price", "Vehicle_Showroom_Delta", "Vehicle_Showroom_Payment_Date", "Average", "Increment", "Target_Currency"]

[col for col in df.columns if col not in sales_input_cols_03]

# COMMAND ----------

sales_input_cols_03 = ["Vehicle_Price_Id", "Vehicle_Showroom_Price", "Vehicle_Showroom_Delta", "Vehicle_Showroom_Payment_Date", "Average", "Increment", "Target_Currency"]

df4 = df.groupby([col for col in df.columns if col not in sales_input_cols_03])\
        .agg(f.collect_list(f.struct(sales_input_cols_03)).alias('SALES_EXPECTATION_03'))\
        .select("SALES_EXPECTATION_03")

display(df4)