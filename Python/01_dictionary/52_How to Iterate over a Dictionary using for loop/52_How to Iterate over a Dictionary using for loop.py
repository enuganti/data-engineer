# Databricks notebook source
# MAGIC %md
# MAGIC - We can **iterate** over
# MAGIC   - **keys** [using **keys()** method]
# MAGIC   - **values** [using **values()** method]
# MAGIC   - **both** [using **item()** method] with a for loop.

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Simple dictionary**

# COMMAND ----------

data = {"name": "Alice", "age": 25, "city": "Bangalore"}

# COMMAND ----------

# MAGIC %md
# MAGIC      # Iterate over keys
# MAGIC      for key in data:
# MAGIC          print(key)
# MAGIC
# MAGIC          (or)
# MAGIC
# MAGIC      for key in data.keys():
# MAGIC      print(key)

# COMMAND ----------

# DBTITLE 1,keys
# Iterate over keys
for key in data:
    print(key)

# COMMAND ----------

# Iterate over keys
for key in data.keys():
    print(key)

# COMMAND ----------

# DBTITLE 1,values()
# Iterate over values
for value in data.values():
    print(value)

# COMMAND ----------

# DBTITLE 1,items()
# Iterate over key-value pairs
for key, value in data.items():
    print(f"Key = {key}, Value = {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Using .items() in Conditional Logic**

# COMMAND ----------

scores = {"Math": 95, "Science": 82, "English": 76, "Social": 70, "Physics":65}

for subject, score in scores.items():
    if score >= 90:
        print(f"{subject}: Excellent")
    elif score >= 75:
        print(f"{subject}: Good")
    else:
        print(f"{subject}: Needs Improvement")

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Nested Dictionaries with .items()**

# COMMAND ----------

employees = {
    "Ashok": {"age": 20, "skill": "Azure", "Exp": 5, "Location": "Bangalore"},
    "Abhijith": {"age": 22, "skill": "AWS", "Exp": 8, "Location": "Chennai"},
    "Nirmala": {"age": 21, "skill": "GCC", "Exp": 7, "Location": "Hyderabad"}
}

for name, details in employees.items():
    print(f"Student: {name}")
    for key, value in details.items():
        print(f"  {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Count word frequency**

# COMMAND ----------

word_count = {"sql": 13, "azure": 15, "aws": 22, "vs code": 17}

for skill, count in word_count.items():
    print(f"There are {count} {skill} students")

# COMMAND ----------

# MAGIC %md
# MAGIC **5) With numbers**

# COMMAND ----------

squares = {1: 1, 2: 4, 3: 9, 4: 16}

for number, square in squares.items():
    print(f"{number} squared = {square}")

# COMMAND ----------

# MAGIC %md
# MAGIC **6) Inside your PySpark case**

# COMMAND ----------

col_type_map = {
    "age": "int",
    "salary": "double",
    "is_active": "boolean",
    "join_date": "date",
    "last_login": "timestamp"
}

for col_name, target_type in col_type_map.items():
    print(f"Column {col_name} should be converted to {target_type}")

# COMMAND ----------

for col_name, target_type in col_type_map.items():
    print(f"Column: {col_name} → Target Type: {target_type}")

# COMMAND ----------

col_type_map = {
    "age": "Int",
    "salary": "DOUBLE",
    "is_active": "Boolean",
    "join_date": "date",
    "last_login": "Timestamp",
    "Exp": "int",
    "Count": "INT"
}

for col_name, target_type in col_type_map.items():
    target_type = target_type.lower()
    print(f"{col_name}: {target_type}")