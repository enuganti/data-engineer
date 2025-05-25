# Databricks notebook source
# MAGIC %md
# MAGIC 1) zip()
# MAGIC 2) zipWithIndex()
# MAGIC 3) zipWithUniqueId()

# COMMAND ----------

# MAGIC %md
# MAGIC **zip**
# MAGIC
# MAGIC - Python zip() is a **built-in** function that takes **zero or more iterable objects** as **arguments** (e.g. **lists, tuples, or sets**) and **aggregates** them in the form of a series of **tuples**.
# MAGIC
# MAGIC - zip() is primarily used for **combining two datasets element-wise**.
# MAGIC
# MAGIC - When creating a PySpark DataFrame from **multiple lists**, ensure that the **lists are aligned correctly**. Each list represents a **column**, and their **lengths should be the same** to avoid data misalignment.
# MAGIC
# MAGIC - The **zip** function is commonly used to **combine multiple lists element-wise**.
# MAGIC
# MAGIC - It creates **tuples**, with **each tuple** containing values from corresponding positions in the input lists.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      # Syntax of zip() function
# MAGIC      zip(iterator1, iterator2, ...)
# MAGIC
# MAGIC **parameters:**
# MAGIC - It takes **iterable** objects as its **arguments**.
# MAGIC
# MAGIC **Return value:**
# MAGIC - It returns a **zip object** which is the **iterable object** of **tuples**.
# MAGIC - If **no argument** is passed into **zip()**, it will return the **empty iterator**.
# MAGIC - If we pass **one argument**, it will return the **iterable of tuples** where **each tuple** has a **single element**.
# MAGIC - If we pass **more than two iterables**, it will return an **iterable of tuples** where **each tuple** contains elements of **all passed iterables**.

# COMMAND ----------

# MAGIC %md
# MAGIC **1) zip() Function without Arguments**

# COMMAND ----------

# DBTITLE 1,Returns an empty list
# Initialize two lists
subjects1 = ["Java","Python","PHP"]
subjects2 = ['C#','CPP','C']

# zip() function with out arguments
final = zip()
print(list(final))

# COMMAND ----------

# MAGIC %md
# MAGIC **2) zip() with Single Iterable as Argument**

# COMMAND ----------

subjects1 = ["Java", "Python", "PHP"]
# Passing single iterable into zip()
final = zip(subjects1)
print(list(final))

# COMMAND ----------

# MAGIC %md
# MAGIC **3) zip() with Two Iterable as Argument**

# COMMAND ----------

# DBTITLE 1,Example 01
# Initialize two lists
subjects1 = ["Java", "Python", "PHP"]
subjects2 = ['C#','CPP','C']
print("List1 :", subjects1)
print("List2 :", subjects2)

# Zip two lists
final = zip(subjects1, subjects2)
print("\nZip Lists :", list(final))

# COMMAND ----------

# DBTITLE 1,Example 02
list1 = ['Adarsh', 'Bibin', 'Chetan', 'Damini', 'Kennedy']
list2 = [30, 25, 35, 28, 29]
list3 = ['India', 'SriLanka', 'Nepal', 'US', 'UK']

rows = list(zip(list1, list2, list3))
rows

# COMMAND ----------

# MAGIC %md
# MAGIC - **zip()** takes corresponding **elements from each list** and groups them into **tuples**.
# MAGIC - **list(zip(...))** converts the **zipped** result into a **list of those tuples**.
# MAGIC
# MAGIC       [
# MAGIC        ('Adarsh', 30, 'India'),
# MAGIC        ('Bibin', 25, 'SriLanka'),
# MAGIC        ('Chetan', 35, 'Nepal'),
# MAGIC        ('Damini', 28, 'US'),
# MAGIC        ('Kennedy', 29, 'UK')
# MAGIC       ]
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([StructField("Name", StringType(), True),
                     StructField("Age", IntegerType(), True),
                     StructField("City", StringType(), True)])

df = spark.createDataFrame(rows, schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Pass Multiple Iterables into Python zip()**

# COMMAND ----------

# Initialize multiple lists
subjects1 = ["Java", "Python", "PHP"]
subjects2 = ['C#','CPP','C']
subjects3 = ['.net','pyspark','scala']
print("List1 :", subjects1)
print("List2 :", subjects2)
print("list3 :", subjects3)

# Zip multiple lists
final = zip(subjects1, subjects2, subjects3)
print("\nZip Lists :", list(final))

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Pass Unequal Lengths of Iterables**
# MAGIC - when we pass the **unequal length** of iterables into **zip()** function, it will return the iterable of **tuples** having **same length** of **least passed iterable**.
# MAGIC - Here, **“html”** is the **extra element**.

# COMMAND ----------

# Initialize the unequal lengths of list
subjects1 = ["Java", "Python", "PHP", "html"]
subjects2 = ['C#','CPP','C']
print("List1 :", subjects1)
print("List2 :", subjects2)

# Zip the unequal lists
final = zip(subjects1, subjects2)
print("zip unequal lists:", list(final))

# COMMAND ----------

# Traversing Parallelly
print("List1:", subjects1)
print("list2:", subjects2)

for i, j in zip(subjects1, subjects2):
    print(i," ",j)

# COMMAND ----------

# MAGIC %md
# MAGIC **6) Unzipping the Iterables**
# MAGIC - **unpack operator (*)** is used to **unzip** the **iterable objects**. If we pass the unpacking operator inside the zip, then iterators will be unzipped.
# MAGIC
# MAGIC **Syntax:**
# MAGIC
# MAGIC      # zip() with unpack operator
# MAGIC      zip(*zipped_data)

# COMMAND ----------

# DBTITLE 1,unpack operator (*)
# Initialize the lists
subjects1 = ["Java", "Python", "PHP", "html"]
subjects2 = ['C#','CPP','C']

final = zip(subjects1, subjects2)
final1 = list(final)

# Unzipping the zipped object
subjects1,subjects2 = zip(*final1)
print("List1:", subjects1)
print("List2:", subjects2)

# COMMAND ----------

# MAGIC %md
# MAGIC **7) Zip iterable Objects into Python Dictionary**

# COMMAND ----------

# DBTITLE 1,dictionary
# Initialize the lists
keys = ["course", "fee", "duration"]
values = ['Python','4000','45 days']
print("List1:", keys)
print("List2:", values)

# Use zip() to convert the dictionary
final = dict(zip(keys, values))
print("\nGet the dictionary using zip():", final)

# COMMAND ----------

# MAGIC %md
# MAGIC **8) Using zip() with RDDs**

# COMMAND ----------

# Create two RDDs
rdd1 = spark.sparkContext.parallelize([1, 2, 3, 4])
rdd2 = spark.sparkContext.parallelize(["a", "b", "c", "d"])

# Zip the RDDs
zipped_rdd = rdd1.zip(rdd2)

# Collect and display results
print(zipped_rdd.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC - **zip()** combines the **two RDDs element-wise**.
# MAGIC - The result is an **RDD of tuples**:
# MAGIC   - **Each tuple** contains **one element** from **rdd1** and the **corresponding element** from **rdd2**.
# MAGIC
# MAGIC         [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')]
# MAGIC
# MAGIC   - **Note:** Both **RDDs** must have the **same number of elements** and be partitioned the same way for zip() to work correctly.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **9) Zipping RDDs with Different Data Types**

# COMMAND ----------

# Create RDDs
rdd_numbers = spark.sparkContext.parallelize([1001, 1002, 1003, 1004, 1005, 1006])
rdd_strings = spark.sparkContext.parallelize(["Admin", "Sales", "Marketing", "HR", "Finance", "Maintenance"])
rdd_booleans = spark.sparkContext.parallelize([True, False, True, False, False, True])

# Zip RDDs
zipped_rdd = rdd_numbers.zip(rdd_strings).zip(rdd_booleans)
print(zipped_rdd.collect())

# COMMAND ----------

# Flatten the tuples and display
flattened_rdd = zipped_rdd.map(lambda x: (x[0][0], x[0][1], x[1]))
print(flattened_rdd.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC      zipped_rdd = rdd_numbers.zip(rdd_strings).zip(rdd_booleans)
# MAGIC
# MAGIC - This is a **two-step zip** process:
# MAGIC
# MAGIC   - **First:** rdd_numbers.zip(rdd_strings)
# MAGIC     - Combines the **first and second** RDDs **element-wise** into **tuples** like:
# MAGIC
# MAGIC           (1001, "Admin"), (1002, "Sales"), ...
# MAGIC
# MAGIC   - Then: **.zip(rdd_booleans)**
# MAGIC
# MAGIC     - Each result from the first zip is now zipped with a boolean value:
# MAGIC
# MAGIC           ((1001, "Admin"), True), ((1002, "Sales"), False), ...
# MAGIC
# MAGIC **Flattening the Tuples:**
# MAGIC
# MAGIC       flattened_rdd = zipped_rdd.map(lambda x: (x[0][0], x[0][1], x[1]))
# MAGIC
# MAGIC       x = ((1001, 'Admin'), True)
# MAGIC       
# MAGIC       (1001, 'Admin', True)
# MAGIC
# MAGIC **Collecting and Printing the Results**
# MAGIC
# MAGIC       print(flattened_rdd.collect())
# MAGIC
# MAGIC **Output**
# MAGIC
# MAGIC      [
# MAGIC        (1001, 'Admin', True),
# MAGIC        (1002, 'Sales', False),
# MAGIC        (1003, 'Marketing', True),
# MAGIC        (1004, 'HR', False),
# MAGIC        (1005, 'Finance', False),
# MAGIC        (1006, 'Maintenance', True)
# MAGIC      ]
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **10) Using zip() for Advanced RDD Transformations**
# MAGIC
# MAGIC - To add **row numbers or indices** to your RDD.
# MAGIC
# MAGIC - **.zipWithIndex()** assigns a **sequential index (starting from 0)** to **each element** in the RDD.
# MAGIC
# MAGIC - It returns a **new RDD of tuples**: each element paired with its corresponding index.
# MAGIC
# MAGIC - Unlike **.zipWithUniqueId()**, the indices are **guaranteed to be 0-based and sequential**.

# COMMAND ----------

# Create an RDD
rdd = spark.sparkContext.parallelize(["kamal", "Bobby", "Senthil", "Dravid"])

# Zip with index
zipped_with_index = rdd.zipWithIndex()

# Collect and display results
print(zipped_with_index.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Using zipWithUniqueId for Unique Identifiers**
# MAGIC
# MAGIC - **.zipWithUniqueId()** pairs each element of the RDD with a **unique long integer ID**.
# MAGIC
# MAGIC - The IDs are **monotonically increasing** and unique, but they are **not guaranteed** to be **sequential** (i.e., **not always 0, 1, 2, 3...**).

# COMMAND ----------

# Create an RDD
rdd = spark.sparkContext.parallelize(["kamal", "Bobby", "Senthil", "Dravid", "Shobha"])

# Zip with unique ID
zipped_with_unique_id = rdd.zipWithUniqueId()

# Collect and display results
print(zipped_with_unique_id.collect())