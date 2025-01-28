# Databricks notebook source
# MAGIC %md
# MAGIC #### **Difference between Choice and Choices**

# COMMAND ----------

help(random.choice)

# COMMAND ----------

help(random.choices)

# COMMAND ----------

import random

# COMMAND ----------

# DBTITLE 1,choice
animals = ['cat', 'dog', 'elephant', 'tiger']

# Select 3 random animals (with replacement)
random_animals = random.choice(animals)
print(f"Randomly selected animals: {random_animals}")

# COMMAND ----------

# DBTITLE 1,choices
animals = ['cat', 'dog', 'elephant', 'tiger']

# Select 3 random animals (with replacement)
random_animals = random.choices(animals)
print(f"Randomly selected animals: {random_animals}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### **a) Select Random Integers**

# COMMAND ----------

# DBTITLE 1,choices k=10
# Numbers from 1 to 20
numbers = list(range(1, 21))
random_numbers = random.choices(numbers, k=10)
print("Random Numbers:", random_numbers)

# COMMAND ----------

# DBTITLE 1,choices k=2, k=4, k=8
animals = ['cat', 'dog', 'elephant', 'tiger']

# Select 2 random animals (with replacement)
random_animals_k2 = random.choices(animals, k=2)
print(f"Randomly selected animals k=2: {random_animals_k2}")

# Select 4 random animals (with replacement)
random_animals_k4 = random.choices(animals, k=4)
print(f"Randomly selected animals k=4: {random_animals_k4}")

# Select 8 random animals (with replacement)
random_animals_k8 = random.choices(animals, k=8)
print(f"Randomly selected animals k=8: {random_animals_k8}")

# COMMAND ----------

# DBTITLE 1,string
# Initialize string
str = 'Python'
rand_num = random.choices(str, k =2)
rand_num

# COMMAND ----------

# DBTITLE 1,weights & k
mylist = ["apple", "banana", "cherry"]
print(random.choices(mylist, weights = [10, 1, 1], k = 14))

# COMMAND ----------

# MAGIC %md
# MAGIC - Control the **probability** of selecting **each item** by providing a **weights parameter**.

# COMMAND ----------

# MAGIC %md
# MAGIC - Return a **list** with **14 items**.
# MAGIC - The **list** should contain a randomly selection of the values from a specified list, and there should be **10 times** higher possibility to select **"apple"** than the other two.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **b) Generate Random Password**
# MAGIC - Generate a random password using **string.ascii_letters and string.digits**.

# COMMAND ----------

import string

characters = string.ascii_letters + string.digits
password = ''.join(random.choices(characters, k=12))
print("Random Password:", password)

# COMMAND ----------

# MAGIC %md
# MAGIC **string.ascii_letters**
# MAGIC - **ascii_letters** constant in Python is part of the **string** module and is a predefined string that contains all the **lowercase and uppercase ASCII letters**. It includes:
# MAGIC
# MAGIC   - **Lowercase letters:** abcdefghijklmnopqrstuvwxyz
# MAGIC   - **Uppercase letters:** ABCDEFGHIJKLMNOPQRSTUVWXYZ
# MAGIC - It is equivalent to the **concatenation** of string.**ascii_lowercase and string.ascii_uppercase**.

# COMMAND ----------

# DBTITLE 1,ascii_letters in Python
import string

# Accessing ascii_letters
s = string.ascii_letters
print(s)

# COMMAND ----------

# DBTITLE 1,Generating a random string
# Generating a random string of length 8 using ascii_letters
# random.choices() selects 8 random characters from ascii_letters.
# The join() method combines these characters into a single string, resulting in a random string of alphabetic characters.

print(random.choices(string.ascii_letters, k=8))
random_string = ''.join(random.choices(string.ascii_letters, k=8))
print(random_string)

# COMMAND ----------

# MAGIC %md
# MAGIC **string.digits**
# MAGIC
# MAGIC - In Python, the **string.digits** constant contains all the digits from **‘0’ to ‘9’**.
# MAGIC - This can be very helpful when we need to work with numbers in string form. The **string.digit** will give the digits **0123456789**.

# COMMAND ----------

# Using string.digits
dig = string.digits
print(dig)

# COMMAND ----------

# Generate a 6-digit OTP
otp = ''.join(random.choices(string.digits, k=6))
print(otp)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **c) Assign Random Categories to Data**
# MAGIC - Assign random categories to a list of items.

# COMMAND ----------

items = ['item1', 'item2', 'item3', 'item4', 'item5']
categories = ['A', 'B', 'C']
random_categories = random.choices(categories, k=len(items))
print(random_categories)

result = list(zip(items, random_categories))
print("Items with Categories:", result)
