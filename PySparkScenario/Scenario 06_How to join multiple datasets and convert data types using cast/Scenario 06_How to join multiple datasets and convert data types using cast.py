# Databricks notebook source
# MAGIC %md
# MAGIC **Example 01**
# MAGIC - JOIN Multiple datasets
# MAGIC - Convert from **integer to string, date to long**

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col, cast

# COMMAND ----------

RunningData_Rev02 = spark.read.option('header',True) \
                              .option("quote", "\"") \
                              .option('InferSchema',True) \
                              .csv("/Volumes/azureadb/pyspark/join/RunningData_Rev02.csv")
display(RunningData_Rev02)

# COMMAND ----------

SalesData_Rev02 = spark.read.option('header',True) \
                            .option("quote", "\"") \
                            .option('InferSchema',True) \
                            .csv("/Volumes/azureadb/pyspark/join/SalesData_Rev02.csv")
display(SalesData_Rev02)

# COMMAND ----------

Sales_Collect_Rev02 = spark.read.option('header',True) \
                                .option("quote", "\"") \
                                .option('InferSchema',True) \
                                .csv("/Volumes/azureadb/pyspark/join/Sales_Collect_Rev02.csv")
display(Sales_Collect_Rev02.limit(20))
print("Total Number of Rows: ", Sales_Collect_Rev02.count())

# COMMAND ----------

Sales_Collect_df = Sales_Collect_Rev02 \
     .join(SalesData_Rev02, how='left', on=[F.col('Target_Simulation_Id') == F.col('Target_Simulation_Identity')]) \
     .join(RunningData_Rev02, how='left', on=['Target_Event_Identity'])
                        
display(Sales_Collect_df.limit(20))

# COMMAND ----------

from pyspark.sql.types import StringType, DoubleType, LongType
from pyspark.sql.functions import to_date

Sales_Collect_df = Sales_Collect_df \
    .withColumn('Id', F.col('Id').cast(StringType())) \
    .withColumn('dept_Id', F.col('dept_Id').cast(StringType())) \
    .withColumn('SubDept_Id', F.col('SubDept_Id').cast(StringType())) \
    .withColumn('Target_Simulation_Id', F.col('Target_Simulation_Id').cast(StringType())) \
    .withColumn('Vehicle_Id', F.col('Vehicle_Id').cast(StringType())) \
    .withColumn('Vehicle_Profile_Id', F.col('Vehicle_Profile_Id').cast(StringType())) \
    .withColumn('Vehicle_Price_Id', F.col('Vehicle_Price_Id').cast(StringType())) \
    .withColumn('Target_Simulation_Identity', F.col('Target_Simulation_Identity').cast(LongType())) \
    .withColumn('Sales_Timestamp', F.col('Sales_Timestamp').cast(LongType())) \
    .withColumn('Vehicle_Delivery_Date', to_date(col('Vehicle_Delivery_Date'), "d-MMM-yy"))

display(Sales_Collect_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 02**
# MAGIC - Convert from **String to Boolean**

# COMMAND ----------

# MAGIC %md
# MAGIC      # withColumn
# MAGIC      df = df.withColumn("age", col("age").cast(StringType())) \
# MAGIC             .withColumn("isGraduated", col("isGraduated").cast(BooleanType())) \
# MAGIC             .withColumn("jobStartDate", col("jobStartDate").cast(DateType()))
# MAGIC      df.printSchema()
# MAGIC
# MAGIC
# MAGIC      # selectExpr()
# MAGIC      df = df.selectExpr("cast(age as int) age",
# MAGIC                         "cast(isGraduated as string) isGraduated",
# MAGIC                         "cast(jobStartDate as string) jobStartDate")
# MAGIC      df.printSchema()
# MAGIC      
# MAGIC
# MAGIC      # SQL expression
# MAGIC      df.createOrReplaceTempView("CastExample")
# MAGIC      df = spark.sql("SELECT STRING(age),BOOLEAN(isGraduated),DATE(jobStartDate) from CastExample")
# MAGIC      df.printSchema()

# COMMAND ----------

data = [("James", 34, "2006-01-01", "true", "M", 3000.60),
        ("Michael", 33, "1980-01-10", "true", "F", 3300.80),
        ("Robert", 37, "1992-01-06", "false", "M", 5000.50)
      ]

columns = ["firstname", "age", "jobStartDate", "isGraduated", "gender", "salary"]

df2 = spark.createDataFrame(data = data, schema = columns)
display(df2)
df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - changes **age** column from **Integer to String (StringType)**
# MAGIC - changes **isGraduated** column from **String to Boolean (BooleanType)**
# MAGIC - changes **jobStartDate** column to from **String to DateType**

# COMMAND ----------

from pyspark.sql.types import StringType, BooleanType, DateType

df3 = df2.withColumn("age", col("age").cast(StringType())) \
         .withColumn("isGraduated", col("isGraduated").cast(BooleanType())) \
         .withColumn("jobStartDate", col("jobStartDate").cast(DateType()))

display(df3)
df3.printSchema()