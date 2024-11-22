# Databricks notebook source
# MAGIC %md
# MAGIC **Scenario:** 
# MAGIC - I have `target table` with full load.
# MAGIC - Now wants to insert `only new records` from `new dataset (.csv)` by comparing with `existing target table`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Method 01**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a table from fullload_uat.csv
# MAGIC CREATE OR REPLACE TABLE full_load_table AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   read_files(
# MAGIC     '/FileStore/tables/initload.csv',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     mode => 'FAILFAST'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM full_load_table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read incrementalload.csv
# MAGIC CREATE OR REPLACE TEMP VIEW incremental_load_view AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   read_files(
# MAGIC     '/FileStore/tables/newload.csv',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     mode => 'FAILFAST'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM incremental_load_view

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert only new records into the table
# MAGIC INSERT INTO full_load_table
# MAGIC SELECT * FROM incremental_load_view ilv
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT * FROM full_load_table flt
# MAGIC   WHERE flt.Cust_Id = ilv.Cust_Id
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM full_load_table

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Method 02**

# COMMAND ----------

# DBTITLE 1,Read target dataset
df_trg = spark.read.csv("/FileStore/tables/initload.csv", header=True, inferSchema=True)
display(df_trg.limit(10))
print("Number of Rows:", df_trg.count())

# COMMAND ----------

# DBTITLE 1,create view
# Assuming df_trg is your DataFrame
df_trg.createOrReplaceTempView("init_load_view")

# Now you can run SQL queries against this temporary view
result_df_trg = spark.sql("SELECT * FROM init_load_view")
display(result_df_trg)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE trg_load_table AS
# MAGIC SELECT * FROM init_load_view

# COMMAND ----------

# DBTITLE 1,read incremental csv file
df_incr = spark.read.csv("/FileStore/tables/newload.csv", header=True, inferSchema=True)
display(df_incr.limit(10))
print("Number of Rows:", df_incr.count())

# COMMAND ----------

# Assuming df_trg is your DataFrame
df_incr.createOrReplaceTempView("new_load_view")

# Now you can run SQL queries against this temporary view
result_df_incr = spark.sql("SELECT * FROM new_load_view")
display(result_df_incr)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE new_load_table AS
# MAGIC SELECT * FROM new_load_view

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO trg_load_table
# MAGIC SELECT
# MAGIC   Company_Name,
# MAGIC   Cust_Id,
# MAGIC   Cust_Name,
# MAGIC   Category,
# MAGIC   Start_Date,
# MAGIC   Start_Cust_Date,
# MAGIC   End_Date,
# MAGIC   Updated_Date,
# MAGIC   Cust_Value,
# MAGIC   Cust_Type,
# MAGIC   Exchange,
# MAGIC   Location,
# MAGIC   Last_Date_UTC,
# MAGIC   Cust_Category,
# MAGIC   Index
# MAGIC FROM
# MAGIC   new_load_table
# MAGIC WHERE
# MAGIC   NOT EXISTS (
# MAGIC     SELECT
# MAGIC       *
# MAGIC     FROM
# MAGIC       trg_load_table t
# MAGIC     WHERE
# MAGIC       new_load_table.Cust_Id = t.Cust_Id
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Method 03**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a table from initload_uat.csv
# MAGIC CREATE OR REPLACE TABLE targetnew_load_table AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   read_files(
# MAGIC     '/FileStore/tables/initload.csv',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     mode => 'FAILFAST'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM targetnew_load_table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   targetnew_load_table (
# MAGIC     Company_Name,
# MAGIC     Cust_Id,
# MAGIC     Cust_Name,
# MAGIC     Category,
# MAGIC     Start_Date,
# MAGIC     Start_Cust_Date,
# MAGIC     End_Date,
# MAGIC     Updated_Date,
# MAGIC     Cust_Value,
# MAGIC     Cust_Type,
# MAGIC     Exchange,
# MAGIC     Location,
# MAGIC     Last_Date_UTC,
# MAGIC     Cust_Category,
# MAGIC     Index
# MAGIC     ) WITH new_load AS (
# MAGIC     SELECT
# MAGIC       *
# MAGIC     FROM
# MAGIC       read_files(
# MAGIC         '/FileStore/tables/newload.csv',
# MAGIC         format => 'csv',
# MAGIC         header => true,
# MAGIC         mode => 'FAILFAST'
# MAGIC       )
# MAGIC   )
# MAGIC SELECT
# MAGIC   Company_Name,
# MAGIC   Cust_Id,
# MAGIC   Cust_Name,
# MAGIC   Category,
# MAGIC   Start_Date,
# MAGIC   Start_Cust_Date,
# MAGIC   End_Date,
# MAGIC   Updated_Date,
# MAGIC   Cust_Value,
# MAGIC   Cust_Type,
# MAGIC   Exchange,
# MAGIC   Location,
# MAGIC   Last_Date_UTC,
# MAGIC   Cust_Category,
# MAGIC   Index
# MAGIC FROM
# MAGIC   new_load
# MAGIC WHERE
# MAGIC   NOT EXISTS (
# MAGIC     SELECT
# MAGIC       *
# MAGIC     FROM
# MAGIC       targetnew_load_table t
# MAGIC     WHERE
# MAGIC       new_load.Cust_Id = t.Cust_Id
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM targetnew_load_table
