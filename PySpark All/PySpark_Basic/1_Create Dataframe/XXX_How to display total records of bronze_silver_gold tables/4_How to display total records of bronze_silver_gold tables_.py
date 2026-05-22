# Databricks notebook source
# MAGIC %md
# MAGIC - To find out duplicate records
# MAGIC - To verify the mismatch of records between bronze, silver & gold

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

full_data_df = spark.read.csv("dbfs:/FileStore/tables/initload.csv", header=True, inferSchema=True)
display(full_data_df.limit(10))

# COMMAND ----------

# DBTITLE 1,bronze
full_data_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "/user/hive/warehouse/bronze") \
    .saveAsTable("employee_bronze")

# COMMAND ----------

# DBTITLE 1,silver
full_data_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "/user/hive/warehouse/silver") \
    .saveAsTable("employee_silver")

# COMMAND ----------

# DBTITLE 1,gold
full_data_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "/user/hive/warehouse/gold") \
    .saveAsTable("employee_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**

# COMMAND ----------

# DBTITLE 1,Bronze Count
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM employee_bronze;

# COMMAND ----------

# DBTITLE 1,Silver Count
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM employee_silver;

# COMMAND ----------

# DBTITLE 1,Gold Count
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM employee_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 02**

# COMMAND ----------

# MAGIC %md
# MAGIC      SELECT COUNT(*) FROM CityTable;
# MAGIC         => All records including NULL Values and Duplicate records
# MAGIC
# MAGIC      SELECT COUNT(City) FROM CityTable;
# MAGIC         => All records including Duplicate records but Excludes NULL Values.
# MAGIC         => Counts all non-NULL values of City, including duplicates.
# MAGIC         => Ignores only the NULL values.
# MAGIC
# MAGIC      SELECT COUNT(DISTINCT City) FROM CityTable;
# MAGIC         => Unique records and Exclude NULL Values.
# MAGIC         => Counts only unique (distinct) non-NULL values of City.
# MAGIC         => Removes duplicates before counting.
# MAGIC         => Also ignores NULL values.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), COUNT(Cust_Id), COUNT(DISTINCT Cust_Id) FROM employee_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), COUNT(Cust_Id), COUNT(DISTINCT Cust_Id) FROM employee_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), COUNT(Cust_Id), COUNT(DISTINCT Cust_Id) FROM employee_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 03**
# MAGIC
# MAGIC      SELECT COUNT(*) FROM azure_dev.bronze_internal.products;
# MAGIC
# MAGIC           azure_dev        =>  Name of catalog (bronze / silver / gold)
# MAGIC           bronze_internal  =>  Name of layer (bronze / silver / gold)
# MAGIC           products         =>  Name of table name                

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   (SELECT COUNT(*) FROM employee_bronze) AS bronze_count,
# MAGIC   (SELECT COUNT(*) FROM employee_silver) AS silver_count,
# MAGIC   (SELECT COUNT(*) FROM employee_gold) AS gold_count;

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 04**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze
# MAGIC SELECT 'Bronze' AS Category, COUNT(*) AS Total, COUNT(Cust_Id) AS Cust_Id_Count, COUNT(DISTINCT Cust_Id) AS Distinct_Cust_Id_Count
# MAGIC FROM employee_bronze
# MAGIC UNION ALL
# MAGIC -- Silver
# MAGIC SELECT 'Silver' AS Category, COUNT(*) AS Total, COUNT(Cust_Id) AS Cust_Id_Count, COUNT(DISTINCT Cust_Id) AS Distinct_Cust_Id_Count
# MAGIC FROM employee_silver
# MAGIC UNION ALL
# MAGIC -- Gold
# MAGIC SELECT 'Gold' AS Category, COUNT(*) AS Total, COUNT(Cust_Id) AS Cust_Id_Count, COUNT(DISTINCT Cust_Id) AS Distinct_Cust_Id_Count
# MAGIC FROM employee_gold