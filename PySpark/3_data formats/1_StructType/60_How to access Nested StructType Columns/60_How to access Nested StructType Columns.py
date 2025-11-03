# Databricks notebook source
# MAGIC %md
# MAGIC #### **Struct Data Type**
# MAGIC
# MAGIC - struct is a **complex data type** that allows the storage of **multiple fields** together within a **single column**.
# MAGIC - The **fields** within a **struct** can be of **different data types** and can be **nested as well**.
# MAGIC - **STRUCT** column contains an **ordered list** of columns called **entries**.
# MAGIC - Each row in the STRUCT column must have the **same keys**.
# MAGIC - STRUCTs are typically used to **nest multiple columns** into a **single column**, and the **nested column** can be of **any type, including other STRUCTs and LISTs**.

# COMMAND ----------

# MAGIC %md
# MAGIC - How to access Nested StructType Columns?
# MAGIC - How to access Complex Nested StructType Columns?
# MAGIC - How to convert csv to struct type and access StructType Columns?

# COMMAND ----------

# MAGIC %md
# MAGIC - struct column named **address** with fields **city and zip** can be defined as:
# MAGIC
# MAGIC       StructType(List(
# MAGIC        StructField("city", StringType, true),
# MAGIC        StructField("zip", IntegerType, true)
# MAGIC       ))
# MAGIC - In this example, **address** is the **struct** column name, **city and zip** are the **fields** of the struct column, their respective data types are **StringType and IntegerType**, and all the fields are **nullable**.

# COMMAND ----------

from pyspark.sql.functions import lit, col, to_json
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, StructType, StructField

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT named_struct('key1', 'value1', 'key2', 42) AS s;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Accessing Nested Struct Columns**

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 01**

# COMMAND ----------

data = [('Rajesh', ('BMW', 'ADF', 'Data Engineer', 5)),
        ('Rajasekar', ('HONDA', 'ADB', 'Developer', 8)),
        ('Harish', ('MARUTI', 'AZURE', 'Testing', 9)),
        ('Kamalesh', ('BENZ', 'PYSPARK', 'Developer', 10)),
        ('Jagadish', ('FORD', 'PYTHON', 'ADE', 3)),
        ('Arijit', ('KIA', 'DEVOPS', 'CI/CD', 4))]

schema_sub = StructType([StructField('make', StringType()), StructField('Technology', StringType()),\
                         StructField('Designation', StringType()), StructField('Experience', StringType())])
                         
schema = StructType([StructField('Name', StringType()), StructField('Description', schema_sub)])

df_nest = spark.createDataFrame(data, schema)
display(df_nest)
df_nest.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **a) Selecting the entire Struct Column**

# COMMAND ----------

# Selecting the entire nested 'address' column
df_nest.select("Description").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Selecting Specific Subfields of a Struct Column**

# COMMAND ----------

# Selecting 'make' subfield within the 'Description' struct column
df_nest.select("Description.make").display()

# COMMAND ----------

# Selecting 'Description' subfield within the 'Description' struct column
df_nest.select("Name", "Description.Technology").display()

# COMMAND ----------

# MAGIC %md
# MAGIC      df.select("name", df["Description.Technology"].alias("Technology"), df["Description.Experience"].alias("Experience"))
# MAGIC                                                       (or)
# MAGIC      df.select("Name", "Description.Technology", "Description.Experience").display()

# COMMAND ----------

# Split struct column into separate columns 
df_nest_01 = df_nest.select("name", df_nest["Description.Technology"].alias("Technology"), df_nest["Description.Experience"].alias("Experience"))
display(df_nest_01)

# COMMAND ----------

# Expand all attributes of "Description"
df_nest.select("*", "Description.*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 02**

# COMMAND ----------

# Data
data = [
        (("jagadish", None, "Smith", 35, 5, "buy"),"chennai","M"),
        (("Anand", "Rose", "", 30, 8, "sell"), "bangalore", "M"),
        (("Julia", "", "Williams", 25, 3, "buy"), "vizak", "F"),
        (("Mukesh", "Bhat", "Royal", 45, 8, "buy"), "madurai", "M"),
        (("Swetha", "Kumari", "Anand", 55, 15, "sell"), "mysore", "F"),
        (("Madan", "Mohan", "Nair", 22, 11, "buy"), "hyderabad", "M"),
        (("George", "", "Williams", 38, 7, "sell"), "London", "M"),
        (("Roshan", "Bhat", "", 41, 3, "buy"), "mandya", "M"),
        (("Sourabh", "Sharma", "", 27, 2, "sell"), "Nasik", "M"),
        (("Mohan", "Rao", "K", 42, 7, "buy"), "nizamabad", "M")
        ]

# Schema
schema_arr = StructType([
    StructField('Name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True),
         StructField('age', IntegerType(), True),
         StructField('experience', IntegerType(), True),
         StructField('status', StringType(), True)
         ])),
     StructField('city', StringType(), True),
     StructField('gender', StringType(), True)
     ])

# Create DataFrame
df_arr = spark.createDataFrame(data=data, schema=schema_arr)
df_arr.printSchema()
display(df_arr)

# COMMAND ----------

# Select struct type
df_arr.select("Name").display()

# COMMAND ----------

# Select columns from struct type
df_arr.select("name.firstname","name.lastname").display()

# COMMAND ----------

# Extract all columns from struct type
df_arr.select("name.*").display()

# COMMAND ----------

# Extract all columns from struct type
df_arr.select("*", "Name.*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 03**

# COMMAND ----------

data = [{"id": 1, "customer_profile": {"name": "Kontext", "age": 3}},
        {"id": 2, "customer_profile": {"name": "Tech", "age": 10}},
        {"id": 3, "customer_profile": {"name": "ADF", "age": 23}},
        {"id": 4, "customer_profile": {"name": "AWS", "age": 30}},
        {"id": 5, "customer_profile": {"name": "GCC", "age": 33}},
        {"id": 6, "customer_profile": {"name": "ADB", "age": 20}}]

customer_schema = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
])

df_schema = StructType([StructField("id", IntegerType(), True), StructField("customer_profile", customer_schema, False)])

df_dict = spark.createDataFrame(data, df_schema)
print(df_dict.schema)
display(df_dict)

# COMMAND ----------

# expand the StructType column
df_dict.select('*', "customer_profile.name", "customer_profile.age").display()

# COMMAND ----------

# explode all attributes
df_dict.select('*', "customer_profile.*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Complex Nested Structures**

# COMMAND ----------

# Create data with nested structure
data = [("James", 34, ("1st Avenue", "New York", "US", ("123-456-7890", "james@example.com"))),
        ("Anna", 23, ("2nd Avenue", "San Francisco", "US", ("987-654-3210", "anna@example.com"))),
        ("Jeff", 45, ("3rd Avenue", "London", "UK", ("456-123-7890", "jeff@example.com"))),
        ("karthik", 34, ("#876", "Chennai", "India", ("9866773221", "karthik@example.com"))),
        ("Anusha", 43, ("48th Main", "Hyderabad", "India", ("9933445500", "anu@example.com"))),
        ("Jayesh", 45, ("3rd Cross", "Colombo", "SriLanka", ("8745612345", "yay@example.com"))),
        ("Paul", 38, ("River View", "Gothenberg", "Sweden", ("656-456-7890", "paul@example.com"))),
        ("Rajesh", 40, ("Cross word", "Berlin", "Germany", ("456-123-2678", "raj@example.com"))),
        ] 

# Defined schema for illustration purposes:
complex_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("contact", StructType([
            StructField("phone", StringType(), True),
            StructField("email", StringType(), True)
        ]), True)
    ]), True)
])

# create DataFrame using createDataFrame method 
df_nested = spark.createDataFrame(data, complex_schema)
display(df_nested)

# COMMAND ----------

# Navigating through multiple levels and selecting 'phone'
df_nested.select("Name","address.contact.phone").display()

# COMMAND ----------

# Navigating through multiple levels and selecting 'phone' & '
df_nested.select("Name", "address.contact.phone", "address.contact.email").display()

# COMMAND ----------

# Expand all columns of "contact"
df_nested.select("address.contact.*").display()

# COMMAND ----------

# Expand all columns of "address"
df_nested.select("address.*").display()

# COMMAND ----------

# Expand all columns of "address"
df_nested.select("address.*", "address.contact.*").display()

# COMMAND ----------

# Expand all columns of "address"
df_nested.select("address.*", "address.contact.*").drop('contact').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) convert csv to struct**

# COMMAND ----------

df_csv = spark.read.csv("dbfs:/FileStore/tables/to_json.csv", header=True, inferSchema=True)
display(df_csv.limit(10))

# COMMAND ----------

# Select all columns and structure them into a single column named 'pp_msg'
df_final = df_csv.select(f.struct('*').alias('pp_msg')).distinct()
display(df_final.limit(10))
