# Databricks notebook source
# MAGIC %md
# MAGIC #### **rand()**
# MAGIC - The **rand()** function in PySpark generates a column with **random values uniformly distributed in the range [0.0, 1.0)**.
# MAGIC - The **range()** function in Python is used for **generating sequences of numbers**.

# COMMAND ----------

from pyspark.sql.functions import rand

# COMMAND ----------

# Create a sample DataFrame
data = [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)]
df = spark.createDataFrame(data, ["id"])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Assigning Random Decimal Values to a New Column**

# COMMAND ----------

from pyspark.sql.functions import rand
# create new column named 'rand' that contains random floats between
df_with_random = df.withColumn("randomValue", rand())
display(df_with_random)

# COMMAND ----------

# create new column named 'rand' that contains random floats between
df_with_random_mult = df.withColumn("randomValue_01", rand())\
                        .withColumn("randomValue_02", rand())\
                        .withColumn("randomValue_03", rand())
display(df_with_random_mult)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Filtering Rows Based on Random Values**

# COMMAND ----------

# Filter rows where random number < 0.5
filtered_df = df.withColumn("RandomNumber", rand()).filter("RandomNumber < 0.5")
display(filtered_df)

# COMMAND ----------

df.withColumn("random_value", (rand() > 0.5)).display()

# COMMAND ----------

df.withColumn("random_value", (rand() < 0.5)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Assigning Random Integer Values to a New Column**

# COMMAND ----------

# MAGIC %md
# MAGIC      df.withColumn('rand_int', round(rand(), 0)).display()
# MAGIC                            (or)
# MAGIC      df.withColumn("random_int", (rand() * 100).cast("integer")).show()

# COMMAND ----------

# DBTITLE 1,Round
from pyspark.sql.functions import rand, round

#create new column named 'rand' that contains random integers
df.withColumn('rand_int', round(rand(), 0)).display()

# COMMAND ----------

# DBTITLE 1,Floor
# Assign random group IDs (1 to 3)
from pyspark.sql.functions import floor

df_with_groups = df.withColumn("GroupID", floor(rand() * 3) + 1)
display(df_with_groups)

# COMMAND ----------

# DBTITLE 1,Create Range
df_range = spark.range(9)
display(df_range)

# COMMAND ----------

# DBTITLE 1,Cast
df_range.withColumn("random_int", (rand() * 100).cast("integer")).display()

# COMMAND ----------

df_cast = df_range.withColumn("rand_01", rand() * 1000)\
                  .withColumn("random_int", (rand() * 1000).cast("integer"))
display(df_cast)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) How to create random decimals / integers between 0 and 100**

# COMMAND ----------

# DBTITLE 1,Generate random decimals b/n 0 to 100
# create new column named 'rand' that contains random floats between 0 and 100
df_with_deci = df.withColumn("random_decimal", rand()*100)
display(df_with_deci)

# COMMAND ----------

# DBTITLE 1,Generate random decimals b/n 0 to 50
# create new column named 'rand' that contains random floats between 0 and 50
df_with_deci50 = df.withColumn("random_decimal", rand()*50)
display(df_with_deci50)

# COMMAND ----------

# DBTITLE 1,Generate random integers b/n 0 to 100
# create new column named 'rand' that contains random floats between 0 and 100
df_with_int = df.withColumn("random_int", round(rand()*100, 0))
display(df_with_int)

# COMMAND ----------

# DBTITLE 1,Generate random decimals b/n 10 to 20
# Generate random numbers between 10 and 20
df_with_random_range = df.withColumn("RandomInRange", rand() * 10 + 10)
display(df_with_random_range)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **5) How to add random noise to existing numeric columns**

# COMMAND ----------

# Create a DataFrame with a numeric column
data = [(1, 100), (2, 200), (3, 300), (4, 400)]
df_numeric = spark.createDataFrame(data, ["ID", "Value"])

from pyspark.sql.functions import col
# Add random noise
df_with_noise = df_numeric.withColumn("ValueWithNoise", col("Value") + rand())
display(df_with_noise)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **6) seed**
# MAGIC - By specifying a value for **seed** within the **rand()** function, we will be able to generate the **same random numbers** each time we run the code.
# MAGIC
# MAGIC        rand(seed=23)
# MAGIC        rand(seed=42)
# MAGIC        df.withColumn('rand_value', rand(seed=23))
# MAGIC        df.withColumn('rand_value', rand(seed=42))
# MAGIC        df.withColumn('rand_value', rand(123))

# COMMAND ----------

# DBTITLE 1,Create Table
# define data
data = [['Mahesh', 18], 
        ['Nitesh', 33], 
        ['Lakshan', 12], 
        ['Kishore', 15], 
        ['Harish', 19],
        ['Watson', 24],
        ['Mohit', 28],
        ['Jagadish', 40],
        ['Tharun', 24],
        ['Supriya', 13]]
  
# define column names
columns = ['team', 'points'] 
  
# create dataframe using data and column names
df_seed = spark.createDataFrame(data, columns) 
  
# view dataframe
display(df_seed)

# COMMAND ----------

from pyspark.sql.functions import rand

#create new column named 'rand' that contains random floats between 0 and 100
df_seed.withColumn('rand_seed_decimal', rand(seed=23)*100).display()

# COMMAND ----------

from pyspark.sql.functions import rand, round

#create new column named 'rand' that contains random integers between 0 and 100
df_seed.withColumn('rand_seed_int', round(rand(seed=23)*100, 0)).display()

# COMMAND ----------

# check out the side to side comparison when running rand without the seed
df_with_rand_seed = df.withColumn("randomValue_01", rand())\
                      .withColumn("randomValue_02", rand())\
                      .withColumn("randomSeed_03", rand(seed=23))\
                      .withColumn("randomSeed_04", rand(seed=23))
display(df_with_rand_seed)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **7) How to partition using range() & rand()?**

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.functions import spark_partition_id

# range(start, end, step, num of partitions)
spark.range(0, 20, 2, 2).withColumn('rand', f.rand(seed=42) * 3)\
                        .withColumn("spart_partition", spark_partition_id()).display()
