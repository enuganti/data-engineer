# Databricks notebook source
# MAGIC %md
# MAGIC #### How to add meta data columns in bronze tables using lit()?
# MAGIC - load_date
# MAGIC - execution_date_time

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("SCHEDULE_DATE", "", "SCHEDULE_DATE")

PARAM_SCHEDULE_DATE = dbutils.widgets.get("SCHEDULE_DATE")

print("PARAM_SCHEDULE_DATE: ", PARAM_SCHEDULE_DATE)

# COMMAND ----------

from datetime import datetime,timezone
from pyspark.sql.functions import lit
from pyspark.sql.types import DateType, TimestampType

# COMMAND ----------

def get_current_utc_datetime():
    dt = datetime.now(timezone.utc)
    strUTCdt = dt.strftime("%Y-%m-%dT%H:%M:%S")
    return strUTCdt

# COMMAND ----------

# MAGIC %md
# MAGIC |  date                             |        conversion                  |
# MAGIC |-----------------------------------|------------------------------------|
# MAGIC |  datetime.now()                   |  2025-08-12 12:27:42.326252        |
# MAGIC |  datetime.now(timezone.utc)       |  2025-08-12 12:34:31.814015+00:00  |
# MAGIC |  dt.strftime("%Y-%m-%dT%H:%M:%S") |  2025-08-12T12:38:32               |

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1) initial load
# MAGIC **a) First execution**

# COMMAND ----------

full_data_df_first = spark.read.csv("/Volumes/workspace/default/@azureadb/from_unixtime.csv", header=True, inferSchema=True)
display(full_data_df_first)

# COMMAND ----------

# full_data_df.show(10,False)
full_data_df_first = full_data_df_first.withColumn("load_date", lit(PARAM_SCHEDULE_DATE).cast(DateType()))
full_data_df_first = full_data_df_first.withColumn("execution_date_time", lit(get_current_utc_datetime()).cast(TimestampType()))

display(full_data_df_first)

# COMMAND ----------

full_data_df_first.createOrReplaceTempView("bronze_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT load_date, execution_date_time, COUNT(*)
# MAGIC FROM bronze_table
# MAGIC GROUP BY load_date, execution_date_time;

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Second execution**

# COMMAND ----------

full_data_df_second = spark.read.csv("/Volumes/workspace/default/@azureadb/from_unixtime_01.csv", header=True, inferSchema=True)
display(full_data_df_second)

# COMMAND ----------

# full_data_df.show(10,False)
full_data_df_second = full_data_df_second.withColumn("load_date", lit(PARAM_SCHEDULE_DATE).cast(DateType()))
full_data_df_second = full_data_df_second.withColumn("execution_date_time", lit(get_current_utc_datetime()).cast(TimestampType()))

display(full_data_df_second)

# COMMAND ----------

uniondf_sec = full_data_df_first.union(full_data_df_second)
display(uniondf_sec)

# COMMAND ----------

uniondf_sec.createOrReplaceTempView("bronze_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT load_date, execution_date_time, COUNT(*)
# MAGIC FROM bronze_table
# MAGIC GROUP BY load_date, execution_date_time;

# COMMAND ----------

# MAGIC %md
# MAGIC **incremental load**

# COMMAND ----------

full_data_df_third = spark.read.csv("/Volumes/workspace/default/@azureadb/from_unixtime_02.csv", header=True, inferSchema=True)
display(full_data_df_third)

# COMMAND ----------

# full_data_df.show(10,False)
full_data_df_third = full_data_df_third.withColumn("load_date", lit(PARAM_SCHEDULE_DATE).cast(DateType()))
full_data_df_third = full_data_df_third.withColumn("execution_date_time", lit(get_current_utc_datetime()).cast(TimestampType()))

display(full_data_df_third)

# COMMAND ----------

uniondf_final = uniondf_sec.union(full_data_df_third)
display(uniondf_final)

# COMMAND ----------

uniondf_final.createOrReplaceTempView("bronze_table")

# COMMAND ----------

# DBTITLE 1,load_date & execution_date_time
# MAGIC %sql
# MAGIC SELECT load_date, execution_date_time, COUNT(*)
# MAGIC FROM bronze_table
# MAGIC GROUP BY load_date, execution_date_time;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze_table
# MAGIC WHERE load_date IS NULL;

# COMMAND ----------

# DBTITLE 1,primary key: task_id
# MAGIC %sql
# MAGIC SELECT task_id, COUNT(*)
# MAGIC FROM bronze_table
# MAGIC GROUP BY task_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze_table
# MAGIC WHERE task_id IS NULL;