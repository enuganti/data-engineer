# Databricks notebook source
# MAGIC %md
# MAGIC #### How to create dataframe (summary table) for bronze, silver & gold tables? 

# COMMAND ----------

# MAGIC %md
# MAGIC | DataFrame	| Row_Count	| Distinct_Count	| Aggregated_Sessions	| Min_Sessions | Max_Sessions	| Avg_Sessions | Null_Sessions | 	Columns_Count |
# MAGIC |-----------|-----------|-----------------|---------------------|--------------|--------------|--------------|---------------|--------------|
# MAGIC | Silver_tbl_01	| 100	| 100	| 8370	| 3	| 195	| 83.7	| 0	| 13 |
# MAGIC | Silver_tbl_02	| 100	| 100	| 7654	| 8	| 191	| 76.54	| 0	| 11 |
# MAGIC | Gold_table	  | 200	| 200	| 16024	| 3	| 195	| 80.12	| 0	| 14 |

# COMMAND ----------

from pyspark.sql.functions import lit, col, sum

# COMMAND ----------

# DBTITLE 1,Silver Table: 01
silver_tbl_01 = spark.read.csv("/Volumes/@azureadb/pyspark/unionby/company_level.csv", header=True, inferSchema=True)
silver_tbl_01 = silver_tbl_01.withColumn("session_id", col("session_id").cast("string"))

print("Column Names of Silver Table 1: \n", silver_tbl_01.columns)
print("\nNo of Columns in Silver Table 1: ", len(silver_tbl_01.columns))
print("\nTotal Rows in Silver Table 01: ", silver_tbl_01.count())

display(silver_tbl_01.limit(10))

# COMMAND ----------

# DBTITLE 1,Silver Table: 02
silver_tbl_02 = spark.read.csv("/Volumes/@azureadb/pyspark/unionby/device_level.csv", header=True, inferSchema=True)

print("Column Names of Silver Table 2: \n", silver_tbl_02.columns)
print("\nNo of Columns in Silver Table 2: ", len(silver_tbl_02.columns))
print("\nTotal Rows in Silver Table 02: ", silver_tbl_02.count())

display(silver_tbl_02.limit(10))

# COMMAND ----------

diff_cols_silver_1_2 = set(silver_tbl_01.columns) - set(silver_tbl_02.columns)
print("Columns in Silver Table 1 but not in Silver Table 2: ", diff_cols_silver_1_2)

# COMMAND ----------

# DBTITLE 1,Gold Table: unionByName
# step:1 adding GRANULARITY column to identify the level of dataframe
df_silver_tbl_01 = silver_tbl_01.withColumn("GRANULARITY", lit("campaign"))

df_silver_tbl_02 = silver_tbl_02 \
    .withColumn("session_id", lit("NULL")) \
    .withColumn("session_name", lit("NULL")) \
    .withColumn("GRANULARITY", lit("device_category"))

# step:2 union of silver dataframes
df_gold_tbl = df_silver_tbl_01.unionByName(df_silver_tbl_02)
display(df_gold_tbl)

# COMMAND ----------

df_gold_tbl.select("session_id").distinct().display()

# COMMAND ----------

df_gold_tbl.select("session_name").distinct().display()

# COMMAND ----------

# DBTITLE 1,create dataframe
from pyspark.sql.functions import sum, min, max, avg, col

data = [
    [
        "Silver_tbl_01",
        silver_tbl_01.count(),
        silver_tbl_01.distinct().count(),
        silver_tbl_01.groupBy().agg(sum("sessions")).collect()[0][0],
        silver_tbl_01.agg(min("sessions")).collect()[0][0],
        silver_tbl_01.agg(max("sessions")).collect()[0][0],
        silver_tbl_01.agg(avg("sessions")).collect()[0][0],
        silver_tbl_01.filter(col("sessions").isNull()).count(),
        len(silver_tbl_01.columns)
    ],
    [
        "Silver_tbl_02",
        silver_tbl_02.count(),
        silver_tbl_02.distinct().count(),
        silver_tbl_02.groupBy().agg(sum("sessions")).collect()[0][0],
        silver_tbl_02.agg(min("sessions")).collect()[0][0],
        silver_tbl_02.agg(max("sessions")).collect()[0][0],
        silver_tbl_02.agg(avg("sessions")).collect()[0][0],
        silver_tbl_02.filter(col("sessions").isNull()).count(),
        len(silver_tbl_02.columns)
    ],
    [
        "Gold_table",
        df_gold_tbl.count(),
        df_gold_tbl.distinct().count(),
        df_gold_tbl.groupBy().agg(sum("sessions")).collect()[0][0],
        df_gold_tbl.agg(min("sessions")).collect()[0][0],
        df_gold_tbl.agg(max("sessions")).collect()[0][0],
        df_gold_tbl.agg(avg("sessions")).collect()[0][0],
        df_gold_tbl.filter(col("sessions").isNull()).count(),
        len(df_gold_tbl.columns)
    ]
]

df_tst = spark.createDataFrame(
    data, 
    ["DataFrame", "Row_Count", "Distinct_Count", "Aggregated_Sessions",
     "Min_Sessions", "Max_Sessions", "Avg_Sessions", "Null_Sessions", "Columns_Count"]
)

display(df_tst)