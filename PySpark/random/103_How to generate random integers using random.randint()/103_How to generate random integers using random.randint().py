# Databricks notebook source
# MAGIC %md
# MAGIC #### **random.randint()**
# MAGIC - The random.randint() function in Python is used to generate the **random integer** from the given range of integers. It takes **start and end** numeric values as its parameters, so that a random integer is generated **within this specified range**.
# MAGIC
# MAGIC - used to generate the random integer value between the **two start and stop ranges**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax**
# MAGIC
# MAGIC      random.randint(start, stop)
# MAGIC
# MAGIC **start**: An integer specifying at which **position to start**.
# MAGIC
# MAGIC **stop**: An integer specifying at which **position to end**.
# MAGIC
# MAGIC **Returns:**
# MAGIC
# MAGIC - A random **integer** in range [start, end] including the **end points**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Errors and Exceptions:**

# COMMAND ----------

# MAGIC %md
# MAGIC **ValueError:**
# MAGIC  - Returns a ValueError when **floating point** values are passed as parameters.

# COMMAND ----------

# DBTITLE 1,ValueError
'''If we pass floating point values as
parameters in the randint() function'''

ValueError = random.randint(1.23, 9.34)
print(ValueError)

# COMMAND ----------

# MAGIC %md
# MAGIC **TypeError:**
# MAGIC  - Returns a TypeError when anything **other than numeric values** are passed as parameters.

# COMMAND ----------

# DBTITLE 1,TypeError
'''If we pass string or character literals as
parameters in the randint() function'''

TypeError = random.randint('a', 'z')
print(TypeError)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **When to Use Which?**
# MAGIC **rand() (from PySpark or NumPy)**
# MAGIC - When working with dataframes in **PySpark or NumPy**, and you need random floating-point numbers between **0.0 and 1.0**.
# MAGIC - **Purpose:**
# MAGIC   - Generates random **floating-point** numbers **between 0.0 and 1.0 (exclusive of 1.0)**.
# MAGIC - **Return Type:**
# MAGIC   - **Float** (e.g., 0.472, 0.891, etc.).
# MAGIC
# MAGIC         # PySpark:
# MAGIC         from pyspark.sql.functions import rand
# MAGIC         df_with_rand = df.withColumn("random_float", rand())
# MAGIC         df_with_rand.show()
# MAGIC
# MAGIC         # NumPy
# MAGIC         import numpy as np
# MAGIC         random_float = np.random.rand()
# MAGIC         print(random_float)  # Example: 0.675493
# MAGIC
# MAGIC **random.randint() (from Python's random module)**
# MAGIC - When you need **random integer** values within a **specific range**.
# MAGIC - **Purpose:**
# MAGIC   - Generates random integer values within a **specified range [a, b] (inclusive of both a and b)**.
# MAGIC - **Return Type:**
# MAGIC   - **Integer** (e.g., 1, 45, 123, etc.).
# MAGIC
# MAGIC         # Python
# MAGIC         import random
# MAGIC         random_int = random.randint(1, 100)  # Generates a random integer between 1 and 100
# MAGIC         print(random_int)  # Example: 45
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Generate a Random Integer Between 1 and 10**

# COMMAND ----------

import random

# COMMAND ----------

# Generate a random integer between 1 and 10
random_number = random.randint(1, 10)
print("Random number between 1 and 10:", random_number)

# COMMAND ----------

# Generate a random integer between -10 and 10
random_number = random.randint(-10, 10)
print("Random number between -10 and 10:", random_number)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Random Integers in a Range with Conditions**

# COMMAND ----------

# Generate a random even number between 2 and 20
random_even = random.randint(1, 10) * 2
print(random_even)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Random Integers to Shuffle a Sequence**

# COMMAND ----------

# Shuffle a list of numbers using random.randint
numbers = list(range(1, 11))
shuffled_numbers = sorted(numbers, key=lambda x: random.randint(1, 100))
print(shuffled_numbers)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) Random Integers for a List**

# COMMAND ----------

# Create a list of 10 random integers between 1 and 50
random_numbers = [random.randint(1, 50) for i in range(10)]
print(random_numbers)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **5) Generate Multiple Random Integers in a Loop**

# COMMAND ----------

# MAGIC %md
# MAGIC      # Generate multiple Random integers
# MAGIC      start = 20
# MAGIC      stop = 40
# MAGIC      for i in range(5):
# MAGIC          print(random.randint(start, stop))
# MAGIC
# MAGIC                 (or)
# MAGIC                 
# MAGIC      # Generate 5 random integers between 1 and 100
# MAGIC      for i in range(5):
# MAGIC          random_number = random.randint(1, 100)
# MAGIC          print("Random number between 1 and 100:", random_number)

# COMMAND ----------

 start = 20
 stop = 40
 for i in range(10):
     print(random.randint(start, stop))

# COMMAND ----------

 # Generate 5 random integers between 1 and 100
 for i in range(5):
     random_number = random.randint(1, 100)
     print("Random number between 1 and 100:", random_number)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **6) Use random.randint() in a Function**

# COMMAND ----------

def generate_random_number(min_value, max_value):
    return random.randint(min_value, max_value)

# Call the function
print("Random number between 50 and 150:", generate_random_number(50, 150))

# COMMAND ----------

def generate_random_numbers(min_value, max_value):
    # Generate 5 random integers between min_value and max_value
    for i in range(5):
        random_number = random.randint(min_value, max_value)
        print(f"Random number between {min_value} and {max_value}: {random_number}")

generate_random_numbers(10, 50)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **7) Generate a Random Integer and Use It in a List Index**

# COMMAND ----------

# Sample list
sample_list = ['apple', 'banana', 'cherry', 'date', 'elderberry']

# Generate a random index to select an item from the list
random_index = random.randint(0, len(sample_list) - 1)
print("Random fruit:", sample_list[random_index])

# COMMAND ----------

# MAGIC %md
# MAGIC #### **8) How to get random integer using particular columns min & max value?**

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/random_data-3.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

from pyspark.sql.functions import min, max

# Find the minimum and maximum Cust_Value
min_value_custValue = df.select(min('Cust_Value')).collect()[0][0]
max_value_custValue = df.select(max('Cust_Value')).collect()[0][0]

print(f"Minimum value: {min_value_custValue}")
print(f"Maximum value: {max_value_custValue}")

# COMMAND ----------

random.randint(min_value_custValue, max_value_custValue)
