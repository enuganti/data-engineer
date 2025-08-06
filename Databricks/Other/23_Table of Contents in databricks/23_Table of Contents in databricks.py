# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1) select, truncate, describe, restore and drop

# COMMAND ----------

# DBTITLE 1,1) check (dev bronze) table
# MAGIC %sql
# MAGIC
# MAGIC -- Bronze
# MAGIC SELECT *, 
# MAGIC        current_date() AS load_date, 
# MAGIC        current_timestamp() AS execution_date_time
# MAGIC FROM asn_dev.bronze.sales_collect_bronze;

# COMMAND ----------

# DBTITLE 1,2) check (dev silver) table
# MAGIC %sql
# MAGIC -- Silver
# MAGIC SELECT * FROM asn_dev.silver.sales_collect_silver;

# COMMAND ----------

# DBTITLE 1,3) check (gold bronze) table
# MAGIC %sql
# MAGIC -- Gold
# MAGIC SELECT * FROM asn_dev.gold.sales_collect_gold;

# COMMAND ----------

# DBTITLE 1,4) Add load_date and execution_date_time
from pyspark.sql.functions import current_date, current_timestamp

if table == "asn_dev.bronze.sales_collect_bronze":
    df = df.withColumn("load_date", current_date()).withColumn("execution_date", current_timestamp())
    display(df)

# COMMAND ----------

# DBTITLE 1,5) update and set value
# MAGIC %sql
# MAGIC UPDATE asn_dev.silver.sales_collect_silver
# MAGIC SET Id = '2700'
# MAGIC WHERE Id = '270';

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE asn_dev.silver.sales_collect_silver
# MAGIC SET Description = 'Matrix'
# MAGIC WHERE Description = 'Engine_Base';

# COMMAND ----------

# DBTITLE 1,6) Describe history
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC -- DESCRIBE HISTORY asn_dev.bronze.sales_collect_bronze;
# MAGIC
# MAGIC -- Silver
# MAGIC DESCRIBE HISTORY asn_dev.silver.sales_collect_silver;
# MAGIC
# MAGIC -- Gold
# MAGIC -- DESCRIBE HISTORY asn_dev.gold.sales_collect_gold;

# COMMAND ----------

# DBTITLE 1,7) describe table
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC DESCRIBE TABLE asn_dev.bronze.sales_collect_bronze;
# MAGIC
# MAGIC -- Silver
# MAGIC -- DESCRIBE HISTORY asn_dev.silver.sales_collect_silver;
# MAGIC
# MAGIC -- Gold
# MAGIC -- DESCRIBE HISTORY asn_dev.gold.sales_collect_gold;

# COMMAND ----------

# DBTITLE 1,8) Select VERSION AS OF
# MAGIC %sql
# MAGIC SELECT * FROM asn_dev.silver.sales_collect_silver VERSION AS OF 2;
# MAGIC -- SELECT * FROM asn_dev.gold.sales_collect_gold VERSION AS OF 1;

# COMMAND ----------

# DBTITLE 1,9) Truncate, Drop
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC -- TRUNCATE TABLE asn_dev.gold.sales_collect_gold;
# MAGIC -- DROP TABLE IF EXISTS asn_dev.gold.sales_collect_gold;
# MAGIC
# MAGIC -- Silver
# MAGIC -- TRUNCATE TABLE asn_dev.silver.sales_collect_silver;
# MAGIC
# MAGIC -- Gold
# MAGIC -- TRUNCATE TABLE asn_dev.gold.sales_collect_gold;

# COMMAND ----------

# DBTITLE 1,10) Restore
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC -- RESTORE asn_dev.gold.sales_collect_gold TO VERSION AS OF 0;
# MAGIC
# MAGIC -- Silver
# MAGIC -- RESTORE asn_dev.silver.sales_collect_silver VERSION AS OF 6;
# MAGIC
# MAGIC -- gold
# MAGIC -- RESTORE asn_dev.gold.sales_collect_gold VERSION AS OF 6;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Check for Count of Bronze/Silver/Gold tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### a) Incremental Load

# COMMAND ----------

# DBTITLE 1,1) Sales
# MAGIC %sql
# MAGIC
# MAGIC -- Bronze
# MAGIC SELECT 'Bronze' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_dev.bronze.sales_collect_bronze
# MAGIC UNION ALL
# MAGIC -- Silver
# MAGIC SELECT 'Silver' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_dev.silver.sales_collect_silver
# MAGIC UNION ALL
# MAGIC --Gold
# MAGIC SELECT 'Gold' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_dev.gold.sales_collect_gold

# COMMAND ----------

# DBTITLE 1,2) Consumer
# MAGIC %sql
# MAGIC
# MAGIC -- Bronze
# MAGIC SELECT 'Bronze' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_dev.bronze.sales_collect_bronze
# MAGIC UNION ALL
# MAGIC -- Silver
# MAGIC SELECT 'Silver' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_dev.silver.sales_collect_silver
# MAGIC UNION ALL
# MAGIC --Gold
# MAGIC SELECT 'Gold' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_dev.gold.sales_collect_gold

# COMMAND ----------

# DBTITLE 1,3) marketing
# MAGIC %sql
# MAGIC
# MAGIC -- Bronze
# MAGIC SELECT 'Bronze' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_dev.bronze.sales_collect_bronze
# MAGIC UNION ALL
# MAGIC -- Silver
# MAGIC SELECT 'Silver' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_dev.silver.sales_collect_silver
# MAGIC UNION ALL
# MAGIC --Gold
# MAGIC SELECT 'Gold' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_dev.gold.sales_collect_gold

# COMMAND ----------

# DBTITLE 1,4) incentive
# MAGIC %sql
# MAGIC
# MAGIC -- Bronze
# MAGIC SELECT 'Bronze' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_tst.bronze.sales_collect_bronze
# MAGIC UNION ALL
# MAGIC -- Silver
# MAGIC SELECT 'Silver' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_tst.silver.sales_collect_silver
# MAGIC UNION ALL
# MAGIC --Gold
# MAGIC SELECT 'Gold' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_tst.gold.sales_collect_gold

# COMMAND ----------

# DBTITLE 1,5) contract
# MAGIC %sql
# MAGIC
# MAGIC -- Bronze
# MAGIC SELECT 'Bronze' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_acc.bronze.sales_collect_bronze
# MAGIC UNION ALL
# MAGIC -- Silver
# MAGIC SELECT 'Silver' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_acc.silver.sales_collect_silver
# MAGIC UNION ALL
# MAGIC --Gold
# MAGIC SELECT 'Gold' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_acc.gold.sales_collect_gold

# COMMAND ----------

# DBTITLE 1,6) Orders
# MAGIC %sql
# MAGIC
# MAGIC -- Bronze
# MAGIC SELECT 'Bronze' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_prd.bronze.sales_collect_bronze
# MAGIC UNION ALL
# MAGIC -- Silver
# MAGIC SELECT 'Silver' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_prd.silver.sales_collect_silver
# MAGIC UNION ALL
# MAGIC --Gold
# MAGIC SELECT 'Gold' AS Category, COUNT(*) AS Total, COUNT(Id) AS ObjectID_Count, COUNT(DISTINCT Id) AS Distinct_ObjectID_Count
# MAGIC FROM asn_prd.gold.sales_collect_gold

# COMMAND ----------

# MAGIC %md
# MAGIC #### b) Full Load

# COMMAND ----------

# DBTITLE 1,7) consumer
# MAGIC %sql
# MAGIC -- Bronze
# MAGIC SELECT COUNT(*) AS Total
# MAGIC FROM asn_prd.bronze.sales_collect_bronze

# COMMAND ----------

# DBTITLE 1,8) consumables
# MAGIC %sql
# MAGIC -- Bronze
# MAGIC SELECT COUNT(*) AS Total
# MAGIC FROM asn_acc.bronze.sales_collect_bronze

# COMMAND ----------

# DBTITLE 1,9) resource type
# MAGIC %sql
# MAGIC -- Bronze
# MAGIC SELECT COUNT(*) AS Total
# MAGIC FROM asn_tst.bronze.sales_collect_bronze

# COMMAND ----------

# DBTITLE 1,10) personel
# MAGIC %sql
# MAGIC -- Bronze
# MAGIC SELECT COUNT(*) AS Total
# MAGIC FROM asn_dev.bronze.sales_collect_bronze

# COMMAND ----------

# DBTITLE 1,11) external
# MAGIC %sql
# MAGIC SELECT 'Id' AS Column_Name,
# MAGIC     COUNT(*) AS Total_Count,
# MAGIC     COUNT(CASE WHEN Id IS NULL THEN 1 END) AS Null_Count,
# MAGIC     COUNT(CASE WHEN Id IS NOT NULL THEN 1 END) AS Non_Null_Count
# MAGIC FROM asn_dev.bronze.sales_collect_bronze;

# COMMAND ----------

# DBTITLE 1,12) restapi
# MAGIC %sql
# MAGIC SELECT 'Id' AS Column_Name,
# MAGIC     COUNT(*) AS Total_Count,
# MAGIC     COUNT(CASE WHEN Id IS NULL THEN 1 END) AS Null_Count,
# MAGIC     COUNT(CASE WHEN Id IS NOT NULL THEN 1 END) AS Non_Null_Count
# MAGIC FROM asn_tst.bronze.sales_collect_bronze;

# COMMAND ----------

# DBTITLE 1,13) sap
# MAGIC %sql
# MAGIC SELECT 'Id' AS Column_Name,
# MAGIC     COUNT(*) AS Total_Count,
# MAGIC     COUNT(CASE WHEN Id IS NULL THEN 1 END) AS Null_Count,
# MAGIC     COUNT(CASE WHEN Id IS NOT NULL THEN 1 END) AS Non_Null_Count
# MAGIC FROM asn_acc.bronze.sales_collect_bronze;

# COMMAND ----------

# DBTITLE 1,14) Order_details
# MAGIC %sql
# MAGIC SELECT 'Id' AS Column_Name,
# MAGIC     COUNT(*) AS Total_Count,
# MAGIC     COUNT(CASE WHEN Id IS NULL THEN 1 END) AS Null_Count,
# MAGIC     COUNT(CASE WHEN Id IS NOT NULL THEN 1 END) AS Non_Null_Count
# MAGIC FROM asn_prd.bronze.sales_collect_bronze;

# COMMAND ----------

# DBTITLE 1,15) resource
# MAGIC %sql
# MAGIC SELECT 'Description' AS Column_Name,
# MAGIC     COUNT(*) AS Total_Count,
# MAGIC     COUNT(CASE WHEN Description IS NULL THEN 1 END) AS Null_Count,
# MAGIC     COUNT(CASE WHEN Description IS NOT NULL THEN 1 END) AS Non_Null_Count
# MAGIC FROM asn_dev.bronze.sales_collect_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Primary Key (Check for Non-NULL values on Primary Key)

# COMMAND ----------

# MAGIC %md
# MAGIC #### a) Incremental Load

# COMMAND ----------

# DBTITLE 1,1) Sales
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC SELECT Id, count(*)
# MAGIC FROM asn_dev.bronze.sales_collect_bronze
# MAGIC GROUP BY Id
# MAGIC HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_dev.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_dev.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_dev.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_dev.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,2) Consumer
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_dev.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_dev.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC
# MAGIC -- silver
# MAGIC
# MAGIC SELECT Id, count(*)
# MAGIC FROM asn_dev.silver.sales_collect_silver
# MAGIC GROUP BY Id
# MAGIC HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_dev.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_dev.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,3) Marketing
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_dev.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_dev.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_dev.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_dev.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC SELECT Id, COUNT(*)
# MAGIC FROM asn_dev.gold.sales_collect_gold
# MAGIC GROUP BY Id
# MAGIC HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,4) incentive
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC SELECT Id, count(*)
# MAGIC FROM asn_tst.bronze.sales_collect_bronze
# MAGIC GROUP BY Id
# MAGIC HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_tst.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_tst.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_tst.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_tst.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,5) contract
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_tst.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_tst.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC SELECT Id, count(*)
# MAGIC FROM asn_tst.silver.sales_collect_silver
# MAGIC GROUP BY Id
# MAGIC HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_tst.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_tst.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,6) Orders
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_tst.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_tst.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_tst.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_tst.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC SELECT Id, COUNT(*)
# MAGIC FROM asn_tst.gold.sales_collect_gold
# MAGIC GROUP BY Id
# MAGIC HAVING COUNT(*)>1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### b) Full Load

# COMMAND ----------

# DBTITLE 1,7) consumer
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_tst.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC SELECT * FROM asn_tst.bronze.sales_collect_bronze
# MAGIC WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_tst.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_tst.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_tst.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,8) consumables
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_tst.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_tst.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_tst.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC SELECT * FROM asn_tst.silver.sales_collect_silver
# MAGIC WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_tst.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,9) resource type
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC SELECT Id, count(*)
# MAGIC FROM asn_acc.bronze.sales_collect_bronze
# MAGIC GROUP BY Id
# MAGIC HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_acc.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_acc.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_acc.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_acc.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,10) personel
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_acc.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_acc.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC SELECT Id, count(*)
# MAGIC FROM asn_acc.silver.sales_collect_silver
# MAGIC GROUP BY Id
# MAGIC HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_acc.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_acc.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,11) external
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_acc.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_acc.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_acc.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_acc.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC SELECT Id, COUNT(*)
# MAGIC FROM asn_acc.gold.sales_collect_gold
# MAGIC GROUP BY Id
# MAGIC HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,12) restapi
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_acc.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC SELECT * FROM asn_acc.bronze.sales_collect_bronze
# MAGIC WHERE Id='502';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_acc.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_acc.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_acc.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,13) sap
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_acc.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_acc.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='502';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_acc.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC SELECT * FROM asn_acc.silver.sales_collect_silver
# MAGIC WHERE Id='502';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_acc.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,14) Order_details
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC SELECT Id, count(*)
# MAGIC FROM asn_prd.bronze.sales_collect_bronze
# MAGIC GROUP BY Id
# MAGIC HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_prd.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_prd.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_prd.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_prd.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,15) resource
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_prd.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_prd.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC SELECT Id, count(*)
# MAGIC FROM asn_prd.silver.sales_collect_silver
# MAGIC GROUP BY Id
# MAGIC HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_prd.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_prd.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,16) detail line item
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_prd.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_prd.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_prd.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_prd.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC SELECT Id, COUNT(*)
# MAGIC FROM asn_prd.gold.sales_collect_gold
# MAGIC GROUP BY Id
# MAGIC HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,17) sample
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_prd.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC SELECT * FROM asn_prd.bronze.sales_collect_bronze
# MAGIC WHERE Id='502';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_prd.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_prd.silver.sales_collect_silver
# MAGIC -- WHERE Id='271';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_prd.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,18) task type
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_prd.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_prd.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_prd.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC SELECT * FROM asn_prd.silver.sales_collect_silver
# MAGIC WHERE Id='502';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_prd.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;

# COMMAND ----------

# DBTITLE 1,19) travel type
# MAGIC %sql
# MAGIC
# MAGIC -- bronze
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_prd.bronze.sales_collect_bronze
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_prd.bronze.sales_collect_bronze
# MAGIC -- WHERE Id='271';
# MAGIC  
# MAGIC -- silver
# MAGIC
# MAGIC -- SELECT Id, count(*)
# MAGIC -- FROM asn_prd.silver.sales_collect_silver
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC -- SELECT * FROM asn_prd.silver.sales_collect_silver
# MAGIC -- WHERE Id='502';
# MAGIC
# MAGIC -- gold
# MAGIC
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_prd.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC SELECT * FROM asn_prd.gold.sales_collect_gold
# MAGIC WHERE Id='502';

# COMMAND ----------

# DBTITLE 1,20) time travel
# MAGIC %sql
# MAGIC -- SELECT Id, COUNT(*)
# MAGIC -- FROM asn_prd.gold.sales_collect_gold
# MAGIC -- GROUP BY Id
# MAGIC -- HAVING COUNT(*)>1;
# MAGIC
# MAGIC SELECT * FROM asn_prd.gold.sales_collect_gold
# MAGIC WHERE Id='461';

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4) Find duplicate records

# COMMAND ----------

# DBTITLE 1,1) manual method: identify & delete duplicates
# MAGIC %sql
# MAGIC
# MAGIC -- SHOW COLUMNS IN asn_prd.gold.sales_collect_gold;
# MAGIC
# MAGIC WITH CTE AS(
# MAGIC     SELECT *,
# MAGIC         row_number() OVER(PARTITION BY Id, dept_Id, SubDept_Id, Vehicle_Id, Vehicle_Profile_Id, Description, Vehicle_Price_Id, Vehicle_Showroom_Price, Vehicle_Showroom_Delta, Vehicle_Showroom_Payment_Date, Currency, Target_Currency, Average, Increment, Target_Simulation_Id ORDER BY Id DESC) as rnk
# MAGIC     FROM asn_prd.gold.sales_collect_gold)
# MAGIC SELECT * FROM CTE
# MAGIC WHERE rnk > 1;
# MAGIC
# MAGIC -- DELETE FROM asn_prd.gold.sales_collect_gold
# MAGIC -- WHERE OBJECT_ID IN (SELECT OBJECT_ID FROM CTE WHERE rnk > 1);

# COMMAND ----------

# DBTITLE 1,2) dynamic method: identify duplicates
table = "asn_dev.bronze.sales_collect_bronze"
# table = "asn_dev.silver.sales_collect_silver"
# table = "asn_dev.gold.sales_collect_gold"
# table = "asn_tst.bronze.sales_collect_bronze"
# table = "asn_tst.silver.sales_collect_silver"
# table = "asn_tst.gold.sales_collect_gold"
# table = "asn_acc.bronze.sales_collect_bronze"
# table = "asn_acc.silver.sales_collect_bronze"
# table = "asn_acc.gold.sales_collect_bronze"
# table = "asn_prd.bronze.sales_collect_bronze"
# table = "asn_prd.silver.sales_collect_silver"
# table = "asn_prd.gold.sales_collect_gold"


df = spark.table(table)
columns = df.columns

# Exclude the ORDER BY column
partition_columns = [col for col in columns if col not in ["Vehicle_Id", "Vehicle_Profile_Id"]] # bronze
# partition_columns = [col for col in columns if col != "eprep_load_ts_utc"] # silver

# Convert to comma-separated string
partition_by_clause = ", ".join(partition_columns)

print(partition_by_clause)

# Create a common CTE query
cte_query = f"""
WITH CTE AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY {partition_by_clause}
            ORDER BY Vehicle_Id DESC
            -- ORDER BY Vehicle_Profile_Id DESC
        ) AS rnk
    FROM {table}
)
"""

# Query to count duplicates
count_query = cte_query + """
SELECT COUNT(*) AS duplicate_count
FROM CTE
WHERE rnk > 1
"""

# Query to display duplicate rows
data_query = cte_query + """
SELECT *
FROM CTE
WHERE rnk > 1
"""

# Execute and display results
display(spark.sql(f"""SELECT COUNT(*) FROM {table}"""))  # Shows count of table
display(spark.sql(count_query))  # Shows count of duplicates
display(spark.sql(data_query))   # Shows the actual duplicate rows