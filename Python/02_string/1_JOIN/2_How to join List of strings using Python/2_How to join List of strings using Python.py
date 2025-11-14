# Databricks notebook source
# MAGIC %md
# MAGIC Python join() is an inbuilt string function used to **join elements** of a sequence **separated by a string separator**. This function joins elements of a sequence and makes it a string.
# MAGIC
# MAGIC **Syntax:**
# MAGIC
# MAGIC      separator_string.join(iterable)
# MAGIC
# MAGIC **Parameters:**
# MAGIC
# MAGIC - **Iterable:** objects capable of returning their members one at a time. Some examples are **List, Tuple, String, Dictionary, and Set**.
# MAGIC
# MAGIC - **Return Value:** The join() method returns a string concatenated with the elements of iterable.
# MAGIC
# MAGIC - **Type Error:** If the iterable contains any **non-string** values, it raises a **TypeError exception**.

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Joining substrings in a string**

# COMMAND ----------

my_string = "python"
separator = ' '
result = separator.join(my_string)
print(result)

# COMMAND ----------

# This will join the characters of the string 'hello' with '-'
str = '-'.join('hello')
print(str)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) How to Join a List into a String?**

# COMMAND ----------

list_abc = ['aaa', 'bbb', 'ccc']
string = '-'.join(list_abc)
print(string)

# COMMAND ----------

# join all the elements in the list using an empty string as a separator
list1 = ['g', 'e', 'e', 'k', 's']
print("".join(list1))

# COMMAND ----------

# join the elements of the list using “$” as a separator
list1 = " geeks "
print("$".join(list1))

# COMMAND ----------

# MAGIC %md
# MAGIC **3) How to Joining a list of Strings with a Custom Separator?**

# COMMAND ----------

words = ["apple", "", "banana", "cherry", ""]
separator = "@ "
result = separator.join(word for word in words if word)
print(result)

# COMMAND ----------

# Joining elements with a custom delimiter
my_list = ['apple', 'banana', 'orange']
delimiter = ' -> '
result = delimiter.join(my_list)
print(result)

# COMMAND ----------

# How to join elements of a list with a space as a separator?
list_of_strings = ['Hello', 'World']
result = ' '.join(list_of_strings)
print(result)

# COMMAND ----------

# How to join elements of a list with a comma as a separator?
list_of_strings = ['apple', 'banana', 'cherry']
result = ','.join(list_of_strings)
print(result)

# COMMAND ----------

list_abc = ['aaa', 'bbb', 'ccc']
string = '\n'.join(list_abc)
print(string)