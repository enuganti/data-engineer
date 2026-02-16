# Databricks notebook source
# MAGIC %md
# MAGIC **1) Handling Non-String Elements**
# MAGIC
# MAGIC - Python expects the elements of the sequence to be **strings**. If any element in the sequence is **not a string**, it will raise a **TypeError**.
# MAGIC
# MAGIC - To handle **non-string elements**, you can convert them to **strings** before using the **join()** method.

# COMMAND ----------

# Using a list comprehension
my_list = [1, 2, 3, 4, 5]
separator = ', '
result = separator.join(str(item) for item in my_list)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Handling missing or empty elements**
# MAGIC
# MAGIC - When the **join()** method encounters **missing or empty** elements in a sequence, it treats them as **empty strings** during concatenation. This behaviour means that missing or empty elements do not disrupt the process of joining other elements. 
# MAGIC
# MAGIC - If you want to handle missing or empty elements differently during joining, you can use conditional statements or filter out those elements before applying the join() method.

# COMMAND ----------

my_list = ['apple', '', 'orange', None, 'grape'] 
separator = ', '
result = separator.join(element for element in my_list if element)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC - In this example, the list **my_list** contains **empty strings and a None value**.
# MAGIC
# MAGIC - By using a **conditional statement** within the generator expression, we **filter out the missing or empty elements (” and None)**.
# MAGIC
# MAGIC - The join() method then **concatenates** the remaining **non-empty** elements using the specified separator.

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Handling Lists with None Values**
# MAGIC
# MAGIC - If your list contains **None** values, you can use a **list comprehension** to remove the **None values or convert them to a string**.

# COMMAND ----------

list = ['Hello', None, 'World']
string = ' '.join([str(i) for i in list if i is not None])
print(string)