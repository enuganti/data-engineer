# Databricks notebook source
# MAGIC %md
# MAGIC #### **random.uniform**
# MAGIC - The random.uniform() function in Python is used to generate a **random floating-point number** within a **specified range**.
# MAGIC
# MAGIC **Syntax**
# MAGIC
# MAGIC       random.uniform(a, b)
# MAGIC       a : (Required) Lower limit of the specified range.
# MAGIC       b : (Required) Upper limit of the specified range.
# MAGIC
# MAGIC       Return value: It returns a random floating number from the specified range.

# COMMAND ----------

help(random.uniform)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Generate a Random Float Between 0 and 1**

# COMMAND ----------

import random
import pyspark.sql.functions as f

# COMMAND ----------

# Generate a random float between 0 and 1
random_float = random.uniform(0, 1)
print("Random float between 0 and 1:", random_float)

# COMMAND ----------

# Generate a random float between 1 and 10
random_float = random.uniform(1, 10)
print("Random float between 1 and 10:", random_float)

# COMMAND ----------

# Generate a Random Float Between -10 and 10
random_float = random.uniform(-10, 10)
print("Random float between -10 and 10:", random_float)

# COMMAND ----------

# DBTITLE 1,loop
# Example 2: Generate multiple floating random numbers
for i in range(5):
    rand_num = random.uniform(10, 20)
    print(rand_num)

# COMMAND ----------

# DBTITLE 1,round
# x is a random floating number between 1 and 100, both included
x = random.uniform(1,100)
print(x)
print(round(x, 4)) # round until last 4 digits
print(round(x, 0))

# COMMAND ----------

# DBTITLE 1,1,1
# Generate a random float between 1 and 1
random_floats = random.uniform(1, 1)
print("Random float between 1 and 1:", random_floats)

# COMMAND ----------

# Generate a random float between 10 and 1
random_fls = random.uniform(10, 1)
print("Random float between 10 and 1:", random_fls)

# COMMAND ----------

print(random.uniform(1, 1.00000000000014))
print(random.uniform(1, 1.0014))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Use random.uniform() in a Function**

# COMMAND ----------

def generate_random_float(min_value, max_value):
    return random.uniform(min_value, max_value)

# Call the function
print("Random float between 50 and 150:", generate_random_float(50, 150))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Generate Multiple Random Floats in a Loop**

# COMMAND ----------

# Generate 5 random floats between 1 and 100
for i in range(5):
    random_float = random.uniform(1, 100)
    print("Random float between 1 and 100:", random_float)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) Generate a Random Float and Use It in a Calculation**

# COMMAND ----------

# Generate a random float between 1 and 10
random_float = random.uniform(1, 10)

# Use the random float in a calculation
result = random_float * 2
print("Random float multiplied by 2:", result)

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/random_data-3.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

# Find the minimum and maximum Base_Last_Sales_Date
min_date_base_last = df.agg(f.min("Base_Last_Sales_Date")).collect()[0][0]
max_date_base_last = df.agg(f.max("Base_Last_Sales_Date")).collect()[0][0]

# COMMAND ----------

random.uniform(min_date_base_last, max_date_base_last)
