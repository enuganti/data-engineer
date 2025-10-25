# Databricks notebook source
df = spark.read.csv("/FileStore/tables/company_level-2.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5) Using .rdd.flatMap()
# MAGIC
# MAGIC - This avoids explicit **list comprehension** and works with **Spark’s RDD**

# COMMAND ----------

# MAGIC %md
# MAGIC ##### a) Convert single column to list

# COMMAND ----------

# convert status_type to list using flatMap
print("status_type: \n", df.select("status_type").rdd.flatMap(lambda x: x).collect())
print("\nDistinct Values of status_type: \n", list(dict.fromkeys(df.select("status_type").rdd.flatMap(lambda x: x).collect())))

# COMMAND ----------

# convert status_name to list using flatMap
print("status_name: \n", df.select("status_name").rdd.flatMap(lambda x: x).collect())
print("\nDistinct Values of status_name: \n", list(dict.fromkeys(df.select("status_name").rdd.flatMap(lambda x: x).collect())))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Convert multiple columns to list

# COMMAND ----------

# convert multiple columns to list using flatMap
print("status_name: \n", df.select(['status_type',
                                    'status_name',
                                    'session_name']).rdd.flatMap(lambda x: x).collect())

print("\nDistinct Values of status_name: \n", list(dict.fromkeys(df.select(['status_type',
                                                                            'status_name',
                                                                            'session_name']).rdd.flatMap(lambda x: x).collect())))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6) Using map()

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 01**

# COMMAND ----------

# Create DataFrame
data = [("James","Smith","USA","CA"),
        ("Michael","Rose","USA","NY"),
        ("Robert","Williams","USA","CA"),
        ("Maria","Jones","USA","FL")
       ]
columns=["firstname","lastname","country","state"]

df_map = spark.createDataFrame(data=data,schema=columns)
display(df_map)

# COMMAND ----------

# PySpark Column to List
states1 = df_map.rdd.map(lambda x: x[3]).collect()
print(states1)

# COMMAND ----------

# Refer column by name you wanted to convert
states2 = df_map.rdd.map(lambda x: x.state).collect()
print(states2)

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 02**

# COMMAND ----------

print(df.select('status_type').rdd.map(lambda x : x).collect())

# COMMAND ----------

df.rdd.map(lambda x : x.status_type).collect()

# COMMAND ----------

list(dict.fromkeys(df.rdd.map(lambda x : x.status_type).collect()))

# COMMAND ----------

# convert  status_type  to list using map
print("status_type: \n", df.select('status_type').rdd.map(lambda x : x[0]).collect())
print("\nDistinct Values of status_type: \n", list(dict.fromkeys(df.select('status_type').rdd.map(lambda x : x[0]).collect())))

# COMMAND ----------

# convert status_name  to list using map
print("status_name: \n", df.select('status_name').rdd.map(lambda x : x[0]).collect())
print("\nDistinct Values of status_name: \n", list(dict.fromkeys(df.select('status_name').rdd.map(lambda x : x[0]).collect())))

# COMMAND ----------

# convert session_name  to list using map
print("session_name: \n", df.select('session_name').rdd.map(lambda x : x[0]).collect())
print("\nDistinct Values of session_name: \n", list(dict.fromkeys(df.select('session_name').rdd.map(lambda x : x[0]).collect())))

# COMMAND ----------

# MAGIC %md
# MAGIC **.rdd**
# MAGIC - Converts the **DataFrame to an RDD** —> a low-level Spark data structure.
# MAGIC - Now, each **row** becomes a **Row object** (like a **small tuple**).
# MAGIC
# MAGIC       [Row(status_type='Not Available'),
# MAGIC        Row(status_type='Not Available'),
# MAGIC        Row(status_type='Not Available')]

# COMMAND ----------

# MAGIC %md
# MAGIC **.map(lambda x: x[0])**
# MAGIC
# MAGIC - Each **x** is a **Row object** that behaves like a **tuple**.
# MAGIC
# MAGIC       x = Row(status_type='Not Available')
# MAGIC
# MAGIC - **x[0]** means **take the first element of the row**.
# MAGIC - So **x[0]** extracts just the value **Not Available**.