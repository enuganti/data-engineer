# Databricks notebook source
# DBTITLE 1,Import Required Libraries PySpark
from pyspark.sql.functions import col, lit, current_timestamp, to_date

# COMMAND ----------

# DBTITLE 1,Import Legecy Funcion
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# DBTITLE 1,Read input data
df = spark.read.option('header',True).option('InferSchema',True).csv("/FileStore/tables/MarketPrice-1.csv")
display(df.limit(10))
df.printSchema()
print("Number of Rows:", df.count())

# COMMAND ----------

# DBTITLE 1,Print data types and count of data
print("Column Names:", df.columns)
print("\nNumber of Columns:", len(df.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **change data type from string to date (Start_Date, End_Date & Effective_Date)**

# COMMAND ----------

# DBTITLE 1,Transformation
df = df.withColumn("Start_Date", to_date(col("Start_Date"), "dd-MMM-yy")) \
       .withColumn("End_Date", to_date(col("End_Date"), "dd-MMM-yy")) \
       .withColumn("Effective_Date", to_date(col("Effective_Date"), "dd-MMM-yy")) \
       .withColumn("created_datetime", current_timestamp()) \
       .withColumn("updated_datetime", current_timestamp()) \
       .withColumn("source_database_id", lit("Azure")) \
       .select(col("Commodity_Index").alias("commodity_index"),
               col("Sensex_Id").alias("sensex_id"),
               col("Sensex_Name").alias("sensex_name"),
               col("Sensex_Category").alias("sensex_category"),
               col("Label_Type").alias("label_type"),
               col("Effective_Date").alias("effective_date"),
               col("Start_Date").alias("start_date"),
               col("End_Date").alias("end_date"),
               col("Income").alias("income"),
               col("Delta_Value").alias("delta_value"),
               col("Target_Id").alias("target_id"),
               col("source_database_id"),
               col("created_datetime"),
               col("updated_datetime")
               )      

display(df.limit(10))
df.printSchema()    

# COMMAND ----------

# drop_cols = ["Price_Effective_Date", "Price_Period_Start_Date", "Price_Period_End_Date"]
# price_point_cols = [col for col in price_point.columns if col not in drop_cols] 

# price_point = price_point.select(
#     *price_point_cols,
#     current_timestamp().alias("created_datetime"),
#     current_timestamp().alias("updated_datetime"),
#     to_date(col("Price_Period_Start_Date"), "dd-MMM-yyyy").alias("Price_Period_Start_Date"),
#     to_date(col("Price_Period_End_Date"), "dd-MMM-yyyy").alias("Price_Period_End_Date"),
#     to_date(col("Price_Effective_Date"), "dd-MMM-yyyy").alias("Price_Effective_Date")
# )

# COMMAND ----------

df.createOrReplaceTempView("MarketPrice_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MarketPrice_tmp LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_enriched_MarketPrice;
# MAGIC CREATE TABLE tbl_enriched_MarketPrice AS 
# MAGIC SELECT mpt.commodity_index,
# MAGIC        mpt.sensex_id,
# MAGIC        mpt.sensex_name,
# MAGIC        mpt.sensex_category,
# MAGIC        mpt.label_type,
# MAGIC        mpt.effective_date,
# MAGIC        mpt.start_date,
# MAGIC        mpt.end_date,
# MAGIC        mpt.income,
# MAGIC        mpt.delta_value,
# MAGIC        mpt.target_id,
# MAGIC        mpt.source_database_id,
# MAGIC        mpt.created_datetime,
# MAGIC        mpt.updated_datetime
# MAGIC FROM MarketPrice_tmp mpt;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tbl_enriched_MarketPrice;