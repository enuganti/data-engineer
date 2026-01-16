# Databricks notebook source
# MAGIC %md
# MAGIC **dict.fromkeys**
# MAGIC - is a built-in Python method used to create a **dictionary from a list** (or any iterable) of **keys**, all initialized with the **same value**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Syntax
# MAGIC      dict.fromkeys(keys, value)
# MAGIC
# MAGIC - **keys** → **list, tuple, or string** (iterable of keys)
# MAGIC - **value** → value assigned to each key (**default = None**)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Basic usage

# COMMAND ----------

# MAGIC %md
# MAGIC      keys = ['id', 'name', 'age', 'city']
# MAGIC      print(dict.fromkeys(keys))
# MAGIC                 (or)
# MAGIC      keys = ['id', 'name', 'age', 'city']
# MAGIC      default_value = None
# MAGIC      dict.fromkeys(keys, default_value)

# COMMAND ----------

# Create dictionary from a list of keys
keys = ['id', 'name', 'age', 'city']

my_dict = dict.fromkeys(keys)
print(my_dict)

# COMMAND ----------

# Create dictionary from a list of keys
keys = ['id', 'name', 'age', 'city']
default_value = None

result = dict.fromkeys(keys, default_value)
print(result)

# COMMAND ----------

status = ['NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'Customer', 'Customer', 'Customer', 'Customer', 'Customer', 'Customer', 'Customer', 'Customer', 'Customer', 'Customer', 'Customer', 'Customer', 'Customer', 'Customer', 'Admin', 'Admin', 'Admin', 'Admin', 'Admin', 'Admin', 'Admin', 'Admin', 'Admin', 'Admin', 'Search', 'Search', 'Search', 'Search', 'Search', 'Search', 'Search', 'Search', 'Search', 'Transport', 'Transport', 'Transport', 'Transport', 'Transport', 'Transport', 'Transport', 'Transport', 'Transport', 'Transport', 'Transport', 'ADLS', 'ADLS', 'ADLS', 'ADLS', 'ADLS', 'ADLS', 'ADLS', 'ADLS', 'ADLS', 'ADLS', 'ADLS', 'Customer', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Marketing', 'Marketing', 'Marketing', 'Marketing', 'Marketing', 'Marketing', 'Marketing', 'Marketing', 'Marketing', 'Marketing', 'Marketing', 'Marketing', 'Marketing']

# COMMAND ----------

# MAGIC %md
# MAGIC      dict.fromkeys(status)
# MAGIC      list(dict.fromkeys(status))
# MAGIC                (or)
# MAGIC      set(status)
# MAGIC      list(set(status))

# COMMAND ----------

dict.fromkeys(status)

# COMMAND ----------

# MAGIC %md
# MAGIC - The **duplicate** entries are **removed**, because **dictionary keys** are **unique**.
# MAGIC - **Removes duplicates** while **preserving** the **original order** of the **list**.

# COMMAND ----------

list(dict.fromkeys(status))

# COMMAND ----------

set(status)

# COMMAND ----------

list(set(status))

# COMMAND ----------

# MAGIC %md
# MAGIC | dict.fromkeys(status) |   set(status)   |
# MAGIC |-----------------------|-----------------|
# MAGIC | {'NA': None,          | {'ADLS',        |
# MAGIC |  'Customer': None,    |  'Admin',       |              
# MAGIC |  'Admin': None,       |  'Customer',    |
# MAGIC |  'Search': None,      |  'Marketing',   |
# MAGIC |  'Transport': None,   |  'NA',          |
# MAGIC |  'ADLS': None,        |  'Sales',       |
# MAGIC |  'Sales': None,       |  'Search',      |
# MAGIC |  'Marketing': None}   |  'Transport'}   |

# COMMAND ----------

# MAGIC %md
# MAGIC | Step | Expression              | Purpose                         | Result                                                    |
# MAGIC | ---- | ----------------------- | ------------------------------- | --------------------------------------------------------- |
# MAGIC | 1    | `status`                | `Original list`                 | `['Active', 'Inactive', 'Active', 'Pending', 'Inactive']` |
# MAGIC | 2    | `dict.fromkeys(status)` | `Remove duplicates (keys only)` | `{'Active': None, 'Inactive': None, 'Pending': None}`     |
# MAGIC | 3    | `list(...)`             | `Convert keys back to list`     | `['Active', 'Inactive', 'Pending']`                       |

# COMMAND ----------

# MAGIC %md
# MAGIC | Method                        | Removes Duplicates | Preserves Order | Example Result       |
# MAGIC | ----------------------------- | ------------------ | --------------- | -------------------- |
# MAGIC | `list(set(status))`           | ✅                  | ❌               | **Order may change**. Which **removes duplicates** but **does not preserve** the **original order**.     |
# MAGIC | `list(dict.fromkeys(status))` | ✅                  | ✅               | **Keeps original order** |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) With a custom default value

# COMMAND ----------

keys = ['id', 'name', 'age', 'math', 'science', 'english']

marks = dict.fromkeys(keys, 0)
print(marks)

# COMMAND ----------

keys = ['id', 'name', 'age', 'math', 'science', 'english']
default_value = 'NA'

result = dict.fromkeys(keys, default_value)
print(result)

# COMMAND ----------

employees = ['E101', 'E102', 'E103', 'E104']

status = dict.fromkeys(employees, 'Pending')
print(status)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Using a tuple as keys

# COMMAND ----------

keys = ('x', 'y', 'z')

coordinates = dict.fromkeys(keys, 10)
print(coordinates)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Using string as keys iterable

# COMMAND ----------

letters = dict.fromkeys('ABC', 1)
print(letters)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) Using a mutable value (like list or dict)

# COMMAND ----------

keys = ['a', 'b', 'c']

data = dict.fromkeys(keys, [])
print(data)

# COMMAND ----------

data['a'].append(1)
print(data)

# COMMAND ----------

# MAGIC %md
# MAGIC - **All keys** share the **same list object**, so **changing one changes all**.
# MAGIC - To fix that, use a dictionary comprehension:
# MAGIC
# MAGIC       data = {k: [] for k in keys}

# COMMAND ----------

data = {k: [] for k in keys}
print(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6) Nested Dictionary

# COMMAND ----------

subjects = ['Math', 'Science', 'English']

marks = dict.fromkeys(subjects, {'score': 0, 'grade': 'N/A'})
print(marks)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary Table
# MAGIC
# MAGIC | Purpose               | Code Example                   | Output                              |
# MAGIC | --------------------- | ------------------------------ | ----------------------------------- |
# MAGIC | Default None          | `dict.fromkeys(['a','b','c'])` | `{'a': None, 'b': None, 'c': None}` |
# MAGIC | Custom value          | `dict.fromkeys(['a','b'], 10)` | `{'a': 10, 'b': 10}`                |
# MAGIC | String keys           | `dict.fromkeys('ABC', 1)`      | `{'A': 1, 'B': 1, 'C': 1}`          |
# MAGIC | Mutable value warning | `dict.fromkeys(['a','b'], [])` | shared list problem                 |