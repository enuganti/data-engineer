# Databricks notebook source
# MAGIC %md
# MAGIC ##### isinstance()
# MAGIC
# MAGIC - `isinstance()` is the safe and recommended way to check `data types` in Python, especially when dealing with `nested or mixed data`.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Syntax
# MAGIC
# MAGIC      isinstance(object, classinfo)
# MAGIC
# MAGIC - **object:** value you want to check
# MAGIC - **classinfo:** type (or tuple of types)
# MAGIC - **Returns:** True or False

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Check basic data types

# COMMAND ----------

print("data type as integer: ", isinstance(10, int))
print("data type as float: ", isinstance(10, float))
print(isinstance(10.5, float))
print(isinstance("hello", str))
print(isinstance(True, bool))
print(isinstance([1, 2, 3], list))
print(isinstance((1, 2, 3), tuple))
print(isinstance({1, 2, 3}, set))
print("data type as dict: ", isinstance({"key": "value"}, dict))
print("data type as list: ", isinstance({"key": "value"}, list))
print(isinstance(None, type(None)))
print(isinstance(None, object))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Using isinstance() in if condition

# COMMAND ----------

value = 100

if isinstance(value, int):
    print("This is an integer")
else:
    print("This is Not an integer")

# COMMAND ----------

value = 100.5

if isinstance(value, int):
    print("This is an integer")
else:
    print("This is Not an integer")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Check multiple types at once

# COMMAND ----------

value = 10.5

if isinstance(value, (int, float)):
    print("Number")

# COMMAND ----------

data = [10, "apple", 3.5, True, None]

for item in data:
    if isinstance(item, str):
        print(item, "→ string")
    # Check bool before int/float because bool is a subclass of int
    elif isinstance(item, bool):
        print(item, "→ boolean")
    elif isinstance(item, (int, float)):
        print(item, "→ number")
    else:
        print(item, "→ unknown")

# COMMAND ----------

data = [{1, 2, 3}, {"name": "Hari"}, [1, 2, 3], (1, 2, 3)]

for item in data:
    if isinstance(item, set):
        print(item, "→ set")
    elif isinstance(item, dict):
        print(item, "→ dictionary")
    elif isinstance(item, list):
        print(item, "→ list")
    elif isinstance(item, tuple):
        print(item, "→ tuple")
    else:
        print(item, "→ unknown")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Using isinstance() with dictionaries

# COMMAND ----------

student = {
    "name": "Ravi",
    "marks": {
        "math": 90,
        "science": 85
    }
}

for key, value in student.items():
    if isinstance(value, dict):
        print(key, "contains another dictionary")
    else:
        print(key, ":", value)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) Using isinstance() with recursion (nested structures)

# COMMAND ----------

def print_dict(d):
    for key, value in d.items():
        if isinstance(value, dict):
            print_dict(value)
        else:
            print(key, ":", value)

# COMMAND ----------

student = {
    "name": "Ravi",
    "age": 25,
    "gender": "male",
    "marks": {
        "math": 90,
        "science": 85
    }
}

# COMMAND ----------

print_dict(student)

# COMMAND ----------

student = {
    "name": "Ravi",
    "age": 25,
    "gender": "male",
    "marks": {
        "math": 90,
        "science": 85,
        "computer": 78,
        "languages": {
            "english": 88,
            "hindi": 92,
            "telugu": 95,
            "tamil": 85,
            "kannada": 80
        }
    }
}

# COMMAND ----------

print_dict(student)

# COMMAND ----------

# MAGIC %md
# MAGIC | Use case              | Example                       |
# MAGIC | --------------------- | ----------------------------- |
# MAGIC | Check single type     | `isinstance(x, int)`          |
# MAGIC | Multiple types        | `isinstance(x, (int, float))` |
# MAGIC | Dict recursion        | `isinstance(v, dict)`         |
# MAGIC | Safer than type()     | ✔                             |
# MAGIC | Used in PySpark logic | ✔                             |