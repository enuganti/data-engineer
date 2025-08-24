# Databricks notebook source
# MAGIC %md
# MAGIC **Example 01**
# MAGIC - JOIN Multiple datasets
# MAGIC - Convert from **integer to string, date to long**

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import col, cast

# COMMAND ----------

RunningData_Rev02 = spark.read.option('header',True).option("quote", "\"").option('InferSchema',True).csv("/FileStore/tables/RunningData_Rev02.csv")
display(RunningData_Rev02)

# COMMAND ----------

SalesData_Rev02 = spark.read.option('header',True).option("quote", "\"").option('InferSchema',True).csv("/FileStore/tables/SalesData_Rev02.csv")
display(SalesData_Rev02)

# COMMAND ----------

Sales_Collect_Rev02 = spark.read.option('header',True).option("quote", "\"").option('InferSchema',True).csv("/FileStore/tables/Sales_Collect_Rev02.csv")
display(Sales_Collect_Rev02)

# COMMAND ----------

Sales_Collect_df = Sales_Collect_Rev02.\
                             join(SalesData_Rev02, how='left',
                                  on=[f.col('Target_Simulation_Id') == f.col('Target_Simulation_Identity')]).\
                             join(RunningData_Rev02, how='left', on=['Target_Event_Identity'])
                        
display(Sales_Collect_df.limit(10))

# COMMAND ----------

#format columns according to datatypes of Kafka Schema
Sales_Collect_df = Sales_Collect_df.\
                         withColumn('Id', f.col('Id').cast(StringType())).\
                         withColumn('dept_Id', f.col('dept_Id').cast(StringType())).\
                         withColumn('SubDept_Id', f.col('SubDept_Id').cast(StringType())).\
                         withColumn('Target_Simulation_Id', f.col('Target_Simulation_Id').cast(StringType())).\
                         withColumn('Vehicle_Id', f.col('Vehicle_Id').cast(StringType())).\
                         withColumn('Vehicle_Profile_Id', f.col('Vehicle_Profile_Id').cast(StringType())).\
                         withColumn('Description', f.col('Description').cast(StringType())).\
                         withColumn('Vehicle_Price_Id', f.col('Vehicle_Price_Id').cast(StringType())).\
                         withColumn('Vehicle_Showroom_Price', f.col('Vehicle_Showroom_Price').cast(DoubleType())).\
                         withColumn('Vehicle_Showroom_Delta', f.col('Vehicle_Showroom_Delta').cast(DoubleType())).\
                         withColumn('Vehicle_Showroom_Payment_Date', f.col('Vehicle_Showroom_Payment_Date').cast(LongType())).\
                         withColumn('Average', f.col('Average').cast(DoubleType())).\
                         withColumn('Increment', f.col('Increment').cast(DoubleType()))

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

data = [("James",34,"2006-01-01","true","M",3000.60),
        ("Michael",33,"1980-01-10","true","F",3300.80),
        ("Robert",37,"06-01-1992","false","M",5000.50)
      ]

columns = ["firstname","age","jobStartDate","isGraduated","gender","salary"]

df2 = spark.createDataFrame(data = data, schema = columns)
display(df2)
df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - changes **age** column from **Integer to String (StringType)**
# MAGIC - changes **isGraduated** column from **String to Boolean (BooleanType)**
# MAGIC - changes **jobStartDate** column to from **String to DateType**

# COMMAND ----------

df3 = df.withColumn("age", col("age").cast(StringType())) \
        .withColumn("isGraduated", col("isGraduated").cast(BooleanType())) \
        .withColumn("jobStartDate", col("jobStartDate").cast(DateType()))
df3.printSchema()