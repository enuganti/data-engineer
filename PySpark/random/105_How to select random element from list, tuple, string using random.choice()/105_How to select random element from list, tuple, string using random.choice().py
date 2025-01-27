# Databricks notebook source
# MAGIC %md
# MAGIC #### **random.choice()**
# MAGIC - The **random.choice()** function in **Python** is used to select a random element from a **non-empty sequence** such as:
# MAGIC   - **list**
# MAGIC   - **nested list**
# MAGIC   - **tuple**
# MAGIC   - **string**
# MAGIC   - **dictionary**

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      random.choice(sequence)
# MAGIC      random.choices(sequence, weights=None, cum_weights=None, k=1)
# MAGIC
# MAGIC - It takes only **one parameter**.
# MAGIC - **sequence**:
# MAGIC   - The sequence must be a **list, tuple, string** etc.
# MAGIC - **weights**:
# MAGIC   - **Optional**. A list were you can weigh the possibility for each value.
# MAGIC      
# MAGIC      **Default None.**
# MAGIC
# MAGIC - **cum_weights**:
# MAGIC   - **Optional**. A list were you can weigh the possibility for each value, only this time the possibility is accumulated.
# MAGIC     
# MAGIC      **Default None**.
# MAGIC
# MAGIC - **k**:
# MAGIC   - **Optional**. An integer defining the length of the returned list
# MAGIC - **Return Value**:
# MAGIC   - It returns a **single random element** from a specified sequence such as a **list, a tuple, a range, a string** etc.
# MAGIC   - If the **sequence is empty**, it will **raise an IndexError**.

# COMMAND ----------

# MAGIC %md
# MAGIC      import random
# MAGIC
# MAGIC      from random import choice

# COMMAND ----------

import random

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Random Selection from a Range**

# COMMAND ----------

# Selects a number between 1 and 100
random_number = random.choice(range(1, 101))
print(f"Randomly selected number: {random_number}")

# COMMAND ----------

new = list(range(1,12))

for i in range(15):
    print(random.choice(new))

# COMMAND ----------

# choose 5 numbers between 1 and 100
mylist = [random.randint(1,100) for i in range(5)]
print(mylist)

# choose a element from mylist
print('choice is', random.choice(mylist))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Select a Random Element from a List**

# COMMAND ----------

# MAGIC %md
# MAGIC - **random.choice** is used to select a random item from the **list**.
# MAGIC - **Each item** in the **list** has an **equal chance** of being selected.
# MAGIC - The function is commonly used in scenarios where you need to make a **random selection from a list of options** or perform random sampling.

# COMMAND ----------

# DBTITLE 1,List
# Sample list
department = ['Admin', 'Finance', 'Sales', 'IT', 'Maintenance', 'Service']
print(department)

# Select a random department
random_department = random.choice(department)
print("Random choice of department:", random_department)

# COMMAND ----------

# DBTITLE 1,Nested List
nested_list = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
random_sublist = random.choice(nested_list)
print(f"Randomly selected sublist: {random_sublist}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Select a Random Character from a String**

# COMMAND ----------

# DBTITLE 1,string
# Sample string
letters = 'abcdefghijklmnopqrstuvwxyz'

# Select a random letter
random_letter = random.choice(letters)
print("Random letter:", random_letter)

# COMMAND ----------

# DBTITLE 1,string
# Initialize the string
# Get the random character
str = 'Python'
for i in range(4):
    rand_num = random.choice(str)
    print("Random element:", rand_num)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) Select a Random Element from a Tuple**

# COMMAND ----------

# DBTITLE 1,tuple
# Sample tuple
numbers = (1, 2, 3, 4, 5)

# Select a random number
random_number = random.choice(numbers)
print("Random number:", random_number)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **5) Get Random Key/Value Pair using Python choice()**
# MAGIC - **random.choice()** upon the **dictionary** to get a **single random key/value pair** from the given dictionary.
# MAGIC - First initialize the dictionary and get the dictionary **keys as list** and pass it as an argument to **choice()**.

# COMMAND ----------

# DBTITLE 1,dictionary
# Create a dictionary
dict = {
    "course": "Python" ,
    "Fee": 4500,
    "duration": '45 days'
}
print("Dictionary:", dict)

# Get a random key from the dictionary.
key = random.choice(list(dict))

# Print the key-value using the key name.
print("Random key/value pair:", key ,":", dict[key])

# COMMAND ----------

# MAGIC %md
# MAGIC #### **6) Simulating a Random Decision**

# COMMAND ----------

decision = random.choice(['Yes', 'No', 'Maybe'])
print(f"Random decision: {decision}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### **7) random.choice() in a Function**

# COMMAND ----------

def get_random_element(sequence):
    return random.choice(sequence)

# Call the function with a list
print("Random element from list:", get_random_element([10, 20, 30, 40, 50]))

# Call the function with a tuple
print("Random element from tuple:", get_random_element((10, 20, 30, 40, 50)))

# Call the function with a range
print("Random element from range:", get_random_element(range(1,100)))

# Call the function with a string
print("Random element from string:", get_random_element('hello'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **8) Select a Random Word from a List of Words**

# COMMAND ----------

# Sample list of words
words = ['python', 'java', 'c++', 'javascript', 'ruby']

# Select a random word
random_word = random.choice(words)
print("Random word:", random_word)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **9) Exception**
# MAGIC - When we pass an **empty sequence** into the **choice()** function, it will **raise an IndexError**. 

# COMMAND ----------

# DBTITLE 1,empty list
# Create a empty list
# Pass empty_list into choice()
empty_list =  []
rand_num = random.choice(empty_list)
print("Random element:", rand_num)

# COMMAND ----------

# DBTITLE 1,integer
# Pass integer into choice() 
# Create a dictionary
integer = 50
rand_num = random.choice(integer)
print("Random element:", rand_num)

# COMMAND ----------

# DBTITLE 1,tuple
tuple = ()
rand_num = random.choice(tuple)
print("Random element:", rand_num)

# COMMAND ----------

# DBTITLE 1,string
string = ''
rand_num = random.choice(string)
print("Random element:", rand_num)

# COMMAND ----------

# DBTITLE 1,dictionary
dictionary = {}
rand_num = random.choice(dictionary)
print("Random element:", rand_num)
