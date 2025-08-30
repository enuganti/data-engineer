# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.functions import min, max, col
from pyspark.sql.types import DateType

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 01**

# COMMAND ----------

# DBTITLE 1,Sample Dataframe
# Sample Data
data = [
    (1, "ITC", 59000, "2024-01-15"),
    (2, "BEML", 68000, "2023-12-10"),
    (3, "HCL", 53500, "2022-06-25"),
    (4, "AIRTEL", 77800, "2021-09-30"),
    (5, "ACT", 5550, "2024-05-15"),
    (6, "TATA", 95600, "2023-09-15"),
    (7, "BEML", 87500, "2025-02-05"),
    (8, "AIRTEL", 95600, "2021-06-20"),
    (9, "ACT", 65000, "2024-02-04"),
    (10, "ITC", 36700, "2022-09-08"),
    (11, "TATA", 175600, "2023-06-15"),
    (12, "ITC", 98700, "2022-12-18"),
    (13, "BEML", 99550, "2023-01-22"),
    (14, "AIRTEL", 395800, "2020-02-23")
]

# Define Schema
columns = ["ID", "Company", "Salary", "JoiningDate"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df = df.withColumn("JoiningDate", col("JoiningDate").cast(DateType()))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Using agg() Function**

# COMMAND ----------

# DBTITLE 1,agg with min & max
# Compute Max and Min using agg()
df.agg(max("Salary").alias("Max_Salary"), min("Salary").alias("Min_Salary")).display()
df.agg(max("JoiningDate").alias("Latest_JoiningDate"), min("JoiningDate").alias("Earliest_JoiningDate")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Using select() with max() and min()**

# COMMAND ----------

# DBTITLE 1,select with min & max
df.select(max("Salary").alias("Max_Salary"), min("Salary").alias("Min_Salary")).display()
df.select(max("JoiningDate").alias("Latest_JoiningDate"), min("JoiningDate").alias("Earliest_JoiningDate")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Using groupBy()**

# COMMAND ----------

# DBTITLE 1,groupby with min & max
df.groupBy("Company").agg(max("Salary").alias("Max_Salary"), min("Salary").alias("Min_Salary")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Using describe()**
# MAGIC - For **Numerical Columns**
# MAGIC - If your column is numeric, describe() provides statistics:
# MAGIC   - **count, mean, stddev, min, and max**.

# COMMAND ----------

# DBTITLE 1,describe
df.select("Salary").describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Summary of Methods:**
# MAGIC
# MAGIC       |-------------------------------|---------------------|-----------------------|--------------------------------|
# MAGIC       |     Method	                  | Works for Integers	| Works for Dates	| Returns                        |
# MAGIC       |-------------------------------|---------------------|-----------------------|--------------------------------|
# MAGIC       | agg(max(), min())             | ✅ Yes              | ✅ Yes	        | Single row with min/max        |
# MAGIC       | select(max(), min())	  | ✅ Yes	        | ✅ Yes	        | Single row with min/max        |
# MAGIC       | groupBy().agg(max(), min())	  | ✅ Yes              | ✅ Yes	        | Min/max per group              |
# MAGIC       | describe()	                  | ✅ Yes	        | ❌ No	                | Statistics including min/max   |
# MAGIC       |-------------------------------|---------------------|-----------------------|--------------------------------|

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 02**

# COMMAND ----------

# DBTITLE 1,sample dataframe
df_sam = spark.read.csv("/FileStore/tables/random_data-2.csv", header=True, inferSchema=True)
display(df_sam)

# COMMAND ----------

# MAGIC %md
# MAGIC      # aggregate
# MAGIC      min_value_custValue = df.agg({'Cust_Value': 'min'})
# MAGIC      max_value_custValue = df.agg({'Cust_Value': 'max'})
# MAGIC                               (or)
# MAGIC      min_value_custValue = df.agg({'Cust_Value': 'min', 'Product_Version_Id': 'min'})
# MAGIC      max_value_custValue = df.agg({'Cust_Value': 'max', 'Product_Version_Id': 'max'})
# MAGIC                               (or)
# MAGIC      min_max_custValue = df.agg(max('Cust_Value').alias('max_sal'), min('Cust_Value').alias('min_sal'))
# MAGIC      
# MAGIC      # collect
# MAGIC      min_value_custValue = df.select(min('Cust_Value')).collect()[0][0]
# MAGIC      max_value_custValue = df.select(max('Cust_Value')).collect()[0][0] 
# MAGIC                               (or)
# MAGIC      min_value_custValue = df.agg(min('Cust_Value').alias('min_Cust_Value')).collect()[0][0]
# MAGIC      max_value_custValue = df.agg(max('Cust_Value').alias('max_Cust_Value')).collect()[0][0]
# MAGIC                               (or)
# MAGIC      min_value_custValue = df.agg(min('Cust_Value').alias('min_Cust_Value')).collect()[0]['min_Cust_Value']
# MAGIC      max_value_custValue = df.agg(max('Cust_Value').alias('max_Cust_Value')).collect()[0]['max_Cust_Value']                      
# MAGIC      
# MAGIC      # first
# MAGIC      min_value_custValue = df.agg(min('Cust_Value').alias('min_Cust_Value')).first()['min_Cust_Value']
# MAGIC      max_value_custValue = df.agg(max('Cust_Value').alias('max_Cust_Value')).first()['max_Cust_Value']

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Integer Columns**

# COMMAND ----------

# MAGIC %md
# MAGIC **a) Using select() with max() and min()**

# COMMAND ----------

# DBTITLE 1,min value: display dataframe
min_value_custValue = df_sam.select(min('Cust_Value').alias('min_Cust_Value'))
max_value_custValue = df_sam.select(max('Cust_Value').alias('max_Cust_Value'))
min_max_custValue = df_sam.agg(max('Cust_Value').alias('max_sal'), min('Cust_Value').alias('min_sal'))

display(min_value_custValue)
display(max_value_custValue)
display(min_max_custValue)

# COMMAND ----------

# MAGIC %md
# MAGIC - In PySpark, the **collect()** method is used to retrieve the results of a **DataFrame action** from the **cluster to the local machine**.
# MAGIC - When you use **select(min('Cust_Value'))**, it creates a **DataFrame** with the minimum value of the Cust_Value column, but it does not actually execute the query and retrieve the result **until you call an action like collect()**.

# COMMAND ----------

# DBTITLE 1,min value: using collect() method
min_value_custValue = df_sam.select(min('Cust_Value')).collect()
max_value_custValue = df_sam.select(max('Cust_Value')).collect()

print(f"Minimum value: {min_value_custValue}")
print(f"Maximum value: {max_value_custValue}")

# COMMAND ----------

min_value_custValue = df_sam.select(min('Cust_Value')).collect()[0][0]
max_value_custValue = df_sam.select(max('Cust_Value')).collect()[0][0]

print(f"Minimum value: {min_value_custValue}")
print(f"Maximum value: {max_value_custValue}")

# COMMAND ----------

# MAGIC %md
# MAGIC - If you want to get the minimum value **without using collect()**, you can use the **agg** method combined with **min** to perform the aggregation and then use **first()** to retrieve the result.
# MAGIC
# MAGIC   - **agg(min('Cust_Value').alias('min_Cust_Value'))** performs the **aggregation** to find the **minimum value**.
# MAGIC   - **first()** retrieves the **first row** of the result.
# MAGIC   - **['min_Cust_Value']** accesses the value of the min_Cust_Value column from the row.

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Using agg() Function**

# COMMAND ----------

# DBTITLE 1,Method 01: min value => using agg & first
min_value_custValue = df_sam.agg(min('Cust_Value').alias('min_Cust_Value')).first()['min_Cust_Value']
print(min_value_custValue)

# COMMAND ----------

df_sam.agg(min('Cust_Value').alias('min_Cust_Value')).display()

# COMMAND ----------

df_sam.agg(min('Cust_Value').alias('min_Cust_Value')).first()

# COMMAND ----------

df_sam.agg(min('Cust_Value').alias('min_Cust_Value')).first()['min_Cust_Value']

# COMMAND ----------

# DBTITLE 1,Method 02: min value =>  using agg & collect
min_value_custValue_collect = df_sam.agg(min('Cust_Value')).collect()[0][0]
print(min_value_custValue_collect)

# COMMAND ----------

# MAGIC %md
# MAGIC **df.select(min('Cust_Value'))**
# MAGIC
# MAGIC - It is typically used to fetch a **scalar result (a single value)** in the context of the DataFrame without requiring explicit grouping.
# MAGIC
# MAGIC **df.agg(min('Cust_Value').alias('min_Cust_Value'))**
# MAGIC
# MAGIC - This is a more formal **aggregation operation** where you use the agg function to **compute summary statistics**, such as the minimum of Cust_Value.
# MAGIC - **Use Case:** This is commonly used when computing **multiple aggregations (e.g., min, max, avg)** or when you want a labeled result for the aggregated values.

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Date**

# COMMAND ----------

# MAGIC %md
# MAGIC **Using agg() Function**

# COMMAND ----------

# Find the minimum and maximum Start_Date
min_date_start = df_sam.agg(f.min("Start_Date")).collect()[0][0]
max_date_start = df_sam.agg(f.max("Start_Date")).collect()[0][0]

print(f"Minimum value: {min_date_start}")
print(f"Maximum value: {max_date_start}")

# COMMAND ----------

# Find the minimum and maximum Base_Start_Date
min_date_base_start = df_sam.agg(f.min("Base_Start_Date")).collect()[0][0]
max_date_base_start = df_sam.agg(f.max("Base_Start_Date")).collect()[0][0]

print(f"Minimum value: {min_date_base_start}")
print(f"Maximum value: {max_date_base_start}")

# COMMAND ----------

# Find the minimum and maximum Base_End_Date
min_date_base_end = df_sam.agg(f.min("Base_End_Date")).collect()[0][0]
max_date_base_end = df_sam.agg(f.max("Base_End_Date")).collect()[0][0]

print(f"Minimum value: {min_date_base_end}")
print(f"Maximum value: {max_date_base_end}")

# COMMAND ----------

# Find the minimum and maximum Base_Expiration_Date
min_date_base_exp = df_sam.agg(f.min("Base_Expiration_Date")).collect()[0][0]
max_date_base_exp = df_sam.agg(f.max("Base_Expiration_Date")).collect()[0][0]

print(f"Minimum value: {min_date_base_exp}")
print(f"Maximum value: {max_date_base_exp}")

# COMMAND ----------

# Find the minimum and maximum Base_Last_Sales_Date
min_date_base_last = df_sam.agg(f.min("Base_Last_Sales_Date")).collect()[0][0]
max_date_base_last = df_sam.agg(f.max("Base_Last_Sales_Date")).collect()[0][0]

print(f"Minimum value: {min_date_base_last}")
print(f"Maximum value: {max_date_base_last}")
