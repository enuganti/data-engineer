# Databricks notebook source
# MAGIC %md
# MAGIC #### How to access dictionary items (keys & values)?
# MAGIC - key()
# MAGIC - values()
# MAGIC - items()
# MAGIC - get()
# MAGIC - index
# MAGIC - setdefault()

# COMMAND ----------

student = {
    "name": "John",
    "age": 20,
    "grade": "A",
    'brand':'audi',
    'model':'q7',
    "subjects": ["Math", "Science", "English"]}
    
print(student)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **1) Accessing keys using:**
# MAGIC - **key() method**
# MAGIC
# MAGIC **Syntax**
# MAGIC
# MAGIC       dict_name.keys()

# COMMAND ----------

student_keys = student.keys()
student_keys

# COMMAND ----------

# MAGIC %md
# MAGIC **How to add new items to a dictionary**

# COMMAND ----------

student["gender"] = "Male"
print(student.keys()) 

# COMMAND ----------

student['fuel type'] = 'diesel'
student_keys

# COMMAND ----------

print(student)

# COMMAND ----------

# MAGIC %md
# MAGIC **Converting to a list**

# COMMAND ----------

print(list(student.keys()))

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Accessing Values using:**
# MAGIC - **index / Key Names / square brackets []**
# MAGIC
# MAGIC **Syntax**
# MAGIC
# MAGIC       dict_name[key]

# COMMAND ----------

student_index = {"name": "Senthil", "age": 20, "grade": "A", 'brand':'finolex', 'model':'abc123'}

print(student_index["name"])
print(student_index["age"])
print(student_index["brand"])
print(student_index["Age"])

# COMMAND ----------

# MAGIC %md
# MAGIC - If the **key doesn’t exist**, this will **raise a KeyError**.

# COMMAND ----------

marks = {1:50, 2:60, 3:70, 4:80, 5:90, 6:100}

print(marks[1])
print(marks[2])
print(marks[3])
print(marks[4])
print(marks[5])
print(marks[6])

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Accessing Values using:**
# MAGIC - **get() method**
# MAGIC
# MAGIC **Syntax**
# MAGIC
# MAGIC       dict_name.get(key)

# COMMAND ----------

department = {"HR": 100, "IT": 200, "Sales": 300}

print(department.get("HR"))                 # Output: 100
print(department.get("IT"))                 # Output: 200
print(department.get("Sales"))              # Output: 300
print(department.get("Marketing"))          # Output: None (no error)
print(department.get("Marketing", "N/A"))   # Output: N/A (default value)

# COMMAND ----------

# MAGIC %md
# MAGIC - **Safer than []**, since it won’t **throw an error** if the **key is missing**.

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Accessing values using:**
# MAGIC - **values() method**
# MAGIC
# MAGIC **Syntax**
# MAGIC
# MAGIC       dict_name.values()

# COMMAND ----------

student = {"name": "John", "age": 20, "grade": "A", 'brand':'audi', 'model':'q7'}

student_values = student.values()
student_values

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Accessing values using:**
# MAGIC - **items() method**
# MAGIC
# MAGIC **Syntax**
# MAGIC
# MAGIC       dict_name.items()

# COMMAND ----------

car = {"brand": "Ford", "model": "Mustang", "year": 1964}
car_items = car.items()
car_items

# COMMAND ----------

car['fuel type'] = 'diesel'
car_items

# COMMAND ----------

print(car)

# COMMAND ----------

# MAGIC %md
# MAGIC **6) Accessing all keys or values**
# MAGIC
# MAGIC - **student.keys()** → returns **all the keys**
# MAGIC - **student.values()** → returns **all the values**
# MAGIC - **student.items()** → returns **key–value pairs as tuples**

# COMMAND ----------

print(student.keys())
print(student.values())
print(student.items())

# COMMAND ----------

# MAGIC %md
# MAGIC **7) setdefault**

# COMMAND ----------

sales = {"product": "Vimson", "orders": 20, "person_name":"nand", "pattern":"quarterly"}

sales.setdefault("salary", 50000)
print(sales)