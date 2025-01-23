# Databricks notebook source
# MAGIC %md
# MAGIC **Ex 01**

# COMMAND ----------

df = spark.range(15)
display(df)

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import rand, when, lit, col
from pyspark.sql.types import LongType

# COMMAND ----------

# Add a column for random values
df1 = df.withColumn('random_value', rand())

# Add a column 'isVal' based on the condition
df2 = df1.withColumn('isVal', when(df1['random_value'] > 0.5, 1).otherwise(0))

# Display the result
display(df2)

# COMMAND ----------

df3 = df2.withColumn('isEven', when((rand() * 10).cast('int') % 2 == 0, 1).otherwise(0)) \
         .withColumn('isHigh', when(rand() > 0.7, 1).otherwise(0))\
         .withColumn('Status', when(rand() < 0.5, 'Low') # 50% chance for "Low"
                              .when((rand() >= 0.5) & (rand() < 0.8), 'Medium') # 30% chance for "Medium"
                              .otherwise('High')) # 20% chance for "High"

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 02**

# COMMAND ----------

product_df = spark.read.csv("/FileStore/tables/cross_join_million-1.csv", header=True, inferSchema=True)
display(product_df)

# COMMAND ----------

# DBTITLE 1,Product_Type
# Group by 'Product_Type' and calculate the count
grouped_df_type_init = product_df.groupBy('Product_Type').count()

# Calculate the total count
total_count = product_df.count()

# Add a percentage column
result_Product_Type_int = grouped_df_type_init.withColumn('Percentage', (col('count') / total_count) * 100)

# Display the result
result_Product_Type_int.display()

# COMMAND ----------

# DBTITLE 1,Product_Flag
# Group by 'Product_Flag' and calculate the count
grouped_df_flag_init = product_df.groupBy('Product_Flag').count()

# Calculate the total count
total_count = product_df.count()

# Add a percentage column
result_Product_Flag_int = grouped_df_flag_init.withColumn('Percentage', (col('count') / total_count) * 100)

# Display the result
result_Product_Flag_int.display()

# COMMAND ----------

# DBTITLE 1,Index
# Group by 'Index' and calculate the count
grouped_df_Index_init = product_df.groupBy('Index').count()

# Calculate the total count
total_count = product_df.count()

# Add a percentage column
result_Index_int = grouped_df_Index_init.withColumn('Percentage', (col('count') / total_count) * 100)

# Display the result
result_Index_int.display()

# COMMAND ----------

# DBTITLE 1,Last_Date
# Group by 'Last_Date' and calculate the count
grouped_df_date_init = product_df.groupBy('Last_Date').count()

# Calculate the total count
total_count = product_df.count()

# Add a percentage column
result_date_int = grouped_df_date_init.withColumn('Percentage', (col('count') / total_count) * 100)

# Display the result
result_date_int.display()

# COMMAND ----------

# DBTITLE 1,End_Date
# Group by 'End_Date' and calculate the count
grouped_df_enddate_init = product_df.groupBy('End_Date').count()

# Calculate the total count
total_count = product_df.count()

# Add a percentage column
result_enddate_int = grouped_df_enddate_init.withColumn('Percentage', (col('count') / total_count) * 100)

# Display the result
result_enddate_int.display()

# COMMAND ----------

df_final = product_df.withColumn('Product_Type', when(rand() < 0.5, 'Automatic').otherwise('Mannual'))\
                     .withColumn('Product_Flag', when(rand() < 0.5, 'Yes').otherwise('No'))\
                     .withColumn("Index", when(rand() < 0.5, 'True').otherwise('False'))\
                     .withColumn('Last_Date', when(rand() < 0.5, lit('1726127367000'))
                                             .otherwise(lit('1727413385000')))\
                     .withColumn('End_Date', when(rand() < 0.12, lit('1717214400000'))
                                            .when(rand() < 0.12, lit('1730437200000'))
                                            .when(rand() < 0.12, lit('1714536000000'))
                                            .when(rand() < 0.12, lit('1725163200000'))
                                            .when(rand() < 0.12, lit('1719806400000'))
                                            .when(rand() < 0.12, lit('1735707600000'))
                                            .when(rand() < 0.12, lit('1743480000000'))
                                            .when(rand() < 0.12, lit('1748728800000'))
                                            .when(rand() < 0.12, lit('1743307200000'))
                                            .otherwise(lit('1767243600000')))\
                     .withColumn("End_Date", f.col("End_Date").cast(LongType()))\
                     .withColumn("Last_Date", f.col("Last_Date").cast(LongType()))\
                     .withColumn("Start_Cust_Date", f.col("Start_Cust_Date").cast(LongType()))

display(df_final)

# COMMAND ----------

# DBTITLE 1,Product_Type
# Group by 'Product_Type' and calculate the count
grouped_df_type = df_final.groupBy('Product_Type').count()

# Calculate the total count
total_count = df_final.count()

# Add a percentage column
result_Product_Type = grouped_df_type.withColumn('Percentage', (col('count') / total_count) * 100)

# Display the result
result_Product_Type.display()

# COMMAND ----------

# DBTITLE 1,Product_Flag
# Group by 'Product_Flag' and calculate the count
grouped_df_flag = df_final.groupBy('Product_Flag').count()

# Calculate the total count
total_count = df_final.count()

# Add a percentage column
result_Product_Flag = grouped_df_flag.withColumn('Percentage', (col('count') / total_count) * 100)

# Display the result
result_Product_Flag.display()

# COMMAND ----------

# DBTITLE 1,Index
# Group by 'Index' and calculate the count
grouped_df_index = df_final.groupBy('Index').count()

# Calculate the total count
total_count = df_final.count()

# Add a percentage column
result_Index = grouped_df_index.withColumn('Percentage', (col('count') / total_count) * 100)

# Display the result
result_Index.display()

# COMMAND ----------

# DBTITLE 1,Last_Date
# Group by 'Last_Date' and calculate the count
grouped_df_date = df_final.groupBy('Last_Date').count()

# Calculate the total count
total_count = df_final.count()

# Add a percentage column
result_Last_Date = grouped_df_date.withColumn('Percentage', (col('count') / total_count) * 100)

# Display the result
result_Last_Date.display()

# COMMAND ----------

# DBTITLE 1,End_Date
# Group by 'End_Date' and calculate the count
grouped_df_enddate = df_final.groupBy('End_Date').count()

# Calculate the total count
total_count = df_final.count()

# Add a percentage column
result_enddate = grouped_df_enddate.withColumn('Percentage', (col('count') / total_count) * 100)

# Display the result
result_enddate.display()
