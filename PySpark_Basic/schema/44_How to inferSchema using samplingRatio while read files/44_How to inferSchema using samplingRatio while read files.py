# Databricks notebook source
# MAGIC %md
# MAGIC #### What is .option("samplingRatio", value)?
# MAGIC
# MAGIC - When you use **inferSchema=True**, Spark **scans** your dataset to **guess data types**.
# MAGIC - But scanning the **entire file** can be **slow for large datasets**.
# MAGIC - By default, Spark **samples the first 1000 rows** to **infer the schema**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Syntax
# MAGIC
# MAGIC      .option("samplingRatio", <value between 0.0 and 1.0>)
# MAGIC
# MAGIC - **0.1** → use **10% of rows** for schema inference.
# MAGIC - **1.0** → use **100%** (default, full scan)
# MAGIC - **0.01** → use only **1% of rows**.

# COMMAND ----------

# MAGIC %md
# MAGIC        .option("samplingRatio", 0.1)
# MAGIC
# MAGIC - This tells Spark to **sample 10% of the rows**. A **higher ratio** means **more accuracy** but **longer loading time**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) For CSV file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### a) Default behavior / Full inference (no samplingRatio)
# MAGIC - Spark reads **all rows** to infer schema.
# MAGIC - **Most accurate**, but **slow for large data**.

# COMMAND ----------

df_def = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/@azureadb/pyspark/dataframe/inferschema.csv")
display(df_def)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Use samplingRatio=0.1 (10%)
# MAGIC - Spark reads only **10% of rows** & Guesses schema from that sample.
# MAGIC - **Much faster** schema inference (especially if file is **very large**)
# MAGIC - Slightly **higher chance** of **incorrect data types**.

# COMMAND ----------

df_ratio_10 = (
    spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("samplingRatio", 0.1) # <-- only 10% rows used to infer schema
        .csv("/Volumes/@azureadb/pyspark/dataframe/inferschema.csv")
)

display(df_ratio_10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### c) Small ratio (1%) for huge dataset
# MAGIC - **Performance gain:** up to **10x faster** schema inference.
# MAGIC - **Risk:** If your **1%** sample **doesn’t contain all data patterns**, types might be **wrong**.

# COMMAND ----------

df_ratio_1 = (
    spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("samplingRatio", 0.01)
        .csv("/Volumes/@azureadb/pyspark/dataframe/inferschema.csv")
)

display(df_ratio_1)

# COMMAND ----------

# MAGIC %md
# MAGIC **Compare Results**
# MAGIC
# MAGIC | Sampling Ratio | Rows Scanned   | Code                             | Accuracy                   | Speed      |
# MAGIC | -------------- | ---------------|--------------------------------- | -------------------------- | ---------- |
# MAGIC | 1.0            | 100%           |Default (no samplingRatio) `.option("samplingRatio", 1.0)`   | ✅ Most accurate           | 🐢 Slowest |
# MAGIC | 0.1            | 10%            |`.option("samplingRatio", 0.1)`   | ⚖️ Good balance (Sometimes less accurate if rare types exist in skipped rows)  | ⚡ Fast  |
# MAGIC | 0.01           | 1%             |`.option("samplingRatio", 0.01)`  | ⚠️ May misinfer some types | 🚀 Fastest |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### d) Observe schema difference

# COMMAND ----------

# MAGIC %md
# MAGIC | age column |
# MAGIC |------------|
# MAGIC | 10 |
# MAGIC | 20 |
# MAGIC | 30 |
# MAGIC | N/A |
# MAGIC | 45.5 |
# MAGIC
# MAGIC If Spark samples only first 3 rows (integers), it might infer:
# MAGIC
# MAGIC      |-- age: integer (nullable = true)
# MAGIC
# MAGIC But in full dataset, 45.5 should make it:
# MAGIC
# MAGIC      |-- age: double (nullable = true)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Best Practice Recommendations
# MAGIC
# MAGIC | Use Case                     | Recommended samplingRatio                    |
# MAGIC | ---------------------------- | -------------------------------------------- |
# MAGIC | Small dataset (<100 MB)      | `1.0` (no sampling)                          |
# MAGIC | Medium dataset (100 MB–1 GB) | `0.2` or `0.3`                               |
# MAGIC | Large dataset (>1 GB)        | `0.05` to `0.1`                              |
# MAGIC | Very large dataset (>10 GB)  | `0.01` or less, but validate schema manually |

# COMMAND ----------

# MAGIC %md
# MAGIC **2) For JSON file**
# MAGIC - Spark will **sample 5%** of **big_data.json** to determine column **data types**.

# COMMAND ----------

# MAGIC %md
# MAGIC      df_json = (
# MAGIC          spark.read
# MAGIC               .option("inferSchema", True)
# MAGIC               .option("samplingRatio", 0.05)
# MAGIC               .json("big_data.json")
# MAGIC      )
# MAGIC
# MAGIC      df_json.printSchema()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **3) With Parquet (ignored)**
# MAGIC - For Parquet, this **option has no effect**, because Parquet files **already store schema** in metadata.
# MAGIC - You **don’t** need **inferSchema or samplingRatio**.

# COMMAND ----------

# MAGIC %md
# MAGIC      df_parquet = (
# MAGIC          spark.read
# MAGIC               .option("samplingRatio", 0.1)
# MAGIC               .parquet("data.parquet")
# MAGIC      )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) When samplingRatio helps performance

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let’s simulate a big dataset:
# MAGIC      df = spark.range(0, 10000000).withColumnRenamed("id", "age")
# MAGIC      df.write.csv("huge_file.csv", header=True)
# MAGIC
# MAGIC ##### Now, read with full and sampled schema inference:
# MAGIC      
# MAGIC      # Full scan (slow)
# MAGIC      df_full = spark.read.option("header", True).option("inferSchema", True).csv("huge_file.csv")
# MAGIC
# MAGIC      # 5% sample scan (faster)
# MAGIC      df_sample = spark.read.option("header", True).option("inferSchema", True).option("samplingRatio", 0.05).csv("huge_file.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC **Result**
# MAGIC
# MAGIC | Setting              | Inference Time (approx) | Accuracy                                      |
# MAGIC | -------------------- | ----------------------- | --------------------------------------------- |
# MAGIC | No samplingRatio     | 12–15 sec               | ✅ 100% accurate                               |
# MAGIC | samplingRatio = 0.05 | 3–4 sec                 | ✅ Same accuracy (since all numeric)           |
# MAGIC | samplingRatio = 0.01 | 1–2 sec                 | ⚠️ Slight risk of mis-inference if mixed data |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Best Practice:
# MAGIC **when:**
# MAGIC - **File is large**,
# MAGIC - You need **quicker schema inference**

# COMMAND ----------

# MAGIC %md
# MAGIC      spark.read.option("header", True).option("inferSchema", True).option("samplingRatio", 0.1).csv("data.csv")