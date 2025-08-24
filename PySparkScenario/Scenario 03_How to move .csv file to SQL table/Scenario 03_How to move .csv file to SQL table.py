# Databricks notebook source
# DBTITLE 1,Import Required Libraries PySpark
from pyspark.sql.functions import col, lit, current_timestamp, to_date

# COMMAND ----------

# DBTITLE 1,Import Legecy Funcion
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# DBTITLE 1,Read input data
df = spark.read.option('header',True).option('InferSchema',True).csv("/FileStore/tables/MarketPrice.csv")
display(df.limit(10))
df.printSchema()
print("Number of Rows:", df.count())

# COMMAND ----------

# DBTITLE 1,Print data types and count of data
print("Column Names:", df.columns)
print("\nNumber of Columns:", len(df.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC #### How to change data type of below columns from string to date?
# MAGIC - Start_Date
# MAGIC - End_Date
# MAGIC - Effective_Date

# COMMAND ----------

# MAGIC %md
# MAGIC ##### to_date()
# MAGIC
# MAGIC 1) Converting a **String** Column with **Default Format**.
# MAGIC    - If your **date strings** follow the **default** format **"yyyy-MM-dd"**, you can simply apply **to_date without specifying a format**.
# MAGIC
# MAGIC 2) Converting a **String** Column with a **Custom Format**
# MAGIC
# MAGIC    - If your **date strings** are in a **different** format (e.g., **"MM/dd/yyyy"**), you **must specify the format** in the **to_date** function.
# MAGIC
# MAGIC |      col_name	        |         format                   | default format: yyyy-MM-dd  |  After to_date(col_name, "yyyy-MM-dd") | correct format |
# MAGIC |-----------------------|----------------------------------|-----------------------------|----------------------------------------|----------------|
# MAGIC | "2024-03-06"	        |  to_date("2024-03-06")           |      Matching               |         2024-03-06 (Date)              | to_date("2024-03-06") |
# MAGIC | "06-03-2024"	        |  to_date("06-03-2024")           |      Not Matching           |       NULL (Format mismatch)           | to_date("06-03-2024", "dd-MM-yyyy") |
# MAGIC | "2024-03-06 12:30:00" |	 to_date("2024-03-06 12:30:00")  |      Not Matching           |      2024-03-06 (Time removed)         | to_date("2024-03-06 12:30:00", "yyyy-MM-dd HH:mm:ss") |

# COMMAND ----------

df_eff = df.withColumn("Start_Date", to_date(col("Start_Date"))) \
           .withColumn("End_Date", to_date(col("End_Date"))) \
           .withColumn("Effective_Date", to_date(col("Effective_Date")))
display(df_eff)

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