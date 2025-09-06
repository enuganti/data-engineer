# Databricks notebook source
# MAGIC %md
# MAGIC #### **Creating MapType**
# MAGIC
# MAGIC - The PySpark **Map Type** datatype is used to represent the map **key-value pair** similar to the python Dictionary **(Dict)**.
# MAGIC  
# MAGIC - A **MapType** column in a Spark DataFrame is one that contains **complex data** in the form of **key-value pairs**, with keys and values each having defined **data types**.
# MAGIC
# MAGIC - In this example, we use **StringType** for **both key and value** and specify **False** to indicate that the **map is not nullable**.
# MAGIC
# MAGIC - The **"keyType" and "valueType"** can be any type that further extends the DataType class for e.g the **StringType, IntegerType, ArrayType, MapType, StructType (struct)** etc.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Why Choose PySpark MapType Dict in Databricks?**
# MAGIC
# MAGIC - PySpark **MapType Dict** allows for the **compact storage** of **key-value pairs** within a **single column**, reducing the overall storage footprint. This becomes crucial when dealing with **large datasets** in a Databricks environment.
# MAGIC
# MAGIC - The MapType Dict provides a **flexible schema, accommodating dynamic and evolving data structures**. This is particularly beneficial in scenarios where the data **schema might change over time**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax**
# MAGIC
# MAGIC      MapType(keyType, valueType, valueContainsNull=True)
# MAGIC
# MAGIC      from pyspark.sql.types import StringType, IntegerType, MapType
# MAGIC      
# MAGIC      MapType(StringType(), IntegerType(), valueContainsNull=True)     
# MAGIC      MapType(StringType(), IntegerType())
# MAGIC      MapType(StringType(), IntegerType(), True)
# MAGIC      MapType(StringType(), StringType(), True)
# MAGIC
# MAGIC **Parameters**
# MAGIC - **keyType:**
# MAGIC   - This is the **data type** of the **keys**.
# MAGIC   - The **keys** in a **MapType** are **not allowed** to be **None or NULL**.
# MAGIC - **valueType:**
# MAGIC   - This is the **data type** of the **values**.
# MAGIC - **valueContainsNull:** 
# MAGIC   - This is a **boolean** value indicating whether the values can be **NULL or None**.
# MAGIC   - The **default** value is **True**, which indicates that the values can be **NULL**.
# MAGIC   - Specify **False** to indicate that the **map is not nullable**.

# COMMAND ----------

from pyspark.sql.functions import lit, col, expr
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, MapType, ArrayType

# COMMAND ----------

help(MapType)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Creating a DataFrame with MapType Schema**

# COMMAND ----------

# MAGIC %md
# MAGIC **key: StringType()**
# MAGIC
# MAGIC **value: IntegerType()**

# COMMAND ----------

# DBTITLE 1,key: string, value: integer
# Sample DataFrame with a StringType column containing JSON strings
data = [("Naresh", "Bangalore", {"Age": 25, "emp_id": 768954, "Exp": 5}), 
        ("Harish", "Chennai", {"Age": 30, "emp_id": 768956, "Exp": 2}),
        ("Prem", "Hyderabad", {"Age": 28, "emp_id": 798954, "Exp": 8}), 
        ("Prabhav", "kochin", {"Age": 35, "emp_id": 788956, "Exp": 6}),
        ("Hari", "Nasik", {"Age": 21, "emp_id": 769954, "Exp": 9}), 
        ("Druv", "Delhi", {"Age": 36, "emp_id": 768946, "Exp": 4}),
        ]

# Define the schema for the MapType column
map_schema = StructType([
  StructField("Name", StringType(), True),
  StructField("City", StringType(), True),
  StructField("Properties", MapType(StringType(), IntegerType()))])

# Convert the StringType column to a MapType column
df_map_int = spark.createDataFrame(data, map_schema)

# Display the resulting DataFrame
display(df_map_int)

# COMMAND ----------

# MAGIC %md
# MAGIC **key : StringType()**
# MAGIC
# MAGIC **value: StringType()**

# COMMAND ----------

# DBTITLE 1,key: string, value: string
# Sample DataFrame with a StringType column containing JSON strings
data = [("Naresh", "Bangalore", {"Domain": "Gas", "Branch": "IT", "Designation": "DE"}), 
        ("Harish", "Chennai", {"Domain": "DS", "Branch": "CSC", "Designation": "DE"}),
        ("Prem", "Hyderabad", {"Domain": "Trade", "Branch": "EEE", "Designation": "DE"}), 
        ("Prabhav", "kochin", {"Domain": "Sales", "Branch": "AI", "Designation": "DE"}),
        ("Hari", "Nasik", {"Domain": "TELE", "Branch": "ECE", "Designation": "DE"}), 
        ("Druv", "Delhi", {"Domain": "BANKING", "Branch": "IT", "Designation": "DE"}),
        ]

# Define the schema for the MapType column
map_schema = StructType([
  StructField("Name", StringType(), True),
  StructField("City", StringType(), True),
  StructField("Properties", MapType(StringType(), StringType()))])

# Convert the StringType column to a MapType column
df_map_str = spark.createDataFrame(data, map_schema)

# Display the resulting DataFrame
display(df_map_str)

# COMMAND ----------

# MAGIC %md
# MAGIC **key : StringType()**
# MAGIC
# MAGIC **value: StringType()**
# MAGIC
# MAGIC **valueContainsNull=True**

# COMMAND ----------

# Sample DataFrame with a StringType column containing JSON strings
data = [("Naresh", "Bangalore", {"Domain": "Gas", "Branch": "IT", "Designation": "DE"}), 
        ("Harish", "Chennai", {"Domain": "DS", "Branch": "CSC", "Designation": None}),
        ("Prem", "Hyderabad", {"Domain": "Trade", "Branch": "EEE", "Designation": "DE"}), 
        ("Prabhav", "kochin", {"Domain": "Sales", "Branch": None, "Designation": "DE"}),
        ("Hari", "Nasik", {"Domain": "TELE", "Branch": "ECE", "Designation": "DE"}), 
        ("Druv", "Delhi", {"Domain": None, "Branch": "IT", "Designation": "DE"}),
        ]

# Define the schema for the MapType column
map_schema = StructType([
  StructField("Name", StringType(), True),
  StructField("City", StringType(), True),
  StructField("Properties", MapType(StringType(), StringType(), True))])

# Convert the StringType column to a MapType column
df_map_str_tr = spark.createDataFrame(data, map_schema)

# Display the resulting DataFrame
display(df_map_str_tr)

# COMMAND ----------

# MAGIC %md
# MAGIC **key : StringType()**
# MAGIC
# MAGIC **value: StringType()**
# MAGIC
# MAGIC **valueContainsNull=False**

# COMMAND ----------

# Sample DataFrame with a StringType column containing JSON strings
data = [("Naresh", "Bangalore", {"Domain": "Gas", "Branch": "IT", "Designation": "DE"}), 
        ("Harish", "Chennai", {"Domain": None, "Branch": "CSC", "Designation": None}),
        ("Prem", "Hyderabad", {"Domain": "Trade", "Branch": "EEE", "Designation": "DE"}), 
        ("Prabhav", "kochin", {"Domain": "Sales", "Branch": None, "Designation": "DE"}),
        ("Hari", "Nasik", {"Domain": "TELE", "Branch": "ECE", "Designation": "DE"}), 
        ("Druv", "Delhi", {"Domain": None, "Branch": "IT", "Designation": "DE"}),
        ]

# Define the schema for the MapType column
map_schema = StructType([
  StructField("Name", StringType(), True),
  StructField("City", StringType(), True),
  StructField("Properties", MapType(StringType(), StringType(), False))])

# Convert the StringType column to a MapType column
df_map_str_fls = spark.createDataFrame(data, map_schema)

# Display the resulting DataFrame
display(df_map_str_fls)

# COMMAND ----------

df_age = df_map_int.select("*", expr("Properties['Age'] + 1").alias('Ages'))
display(df_age)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Creating a DataFrame with Nested MapType Schema**

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating MapType from StructType**
# MAGIC
# MAGIC - We can create a more complex schema using **StructType and StructField**. This is useful when the data involves **nested structures**.

# COMMAND ----------

# DBTITLE 1,Nested MapType
# Define the schema for the DataFrame
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Details", MapType(StringType(), StructType([
      StructField("Age", IntegerType(), True),
      StructField("Pin", IntegerType(), True),
      StructField("City", StringType(), True),
      StructField("Gender", StringType(), True),
      StructField("DOB", StringType(), True),
      StructField("Fees", IntegerType(), True),
      StructField("Experience", IntegerType(), True),
      StructField("Address", StringType(), True)])), True)
    ])

# Sample data
data = [
    ("John", {"personal_info": {"Age": 30, "Pin": 517132, "City": "Bangalore", "Gender": "Male", "DOB": "16-061981", "Fees": 200000, "Experience": 5, "Address": "123 Main St"}}),
    ("Jaswanth", {"personal_info": {"Age": 25, "Pin": 527332, "City": "Hyderabad", "Gender": "Male", "DOB": "16-061981", "Fees": 250000, "Experience": 8, "Address": "456 Maple Ave"}}),
    ("Dinesh", {"personal_info": {"Age": 28, "Pin": 537432, "City": "Chennai", "Gender": "Male", "DOB": "16-061981", "Fees": 350000, "Experience": 3, "Address": "789 Elm St"}}),
    ("Watson", {"personal_info": {"Age": 30, "Pin": 557672, "City": "Nasik", "Gender": "Male", "DOB": "16-061981", "Fees": 3550000, "Experience": 6, "Address": "451 27th Main"}}),
    ("David", {"personal_info": {"Age": 25, "Pin": 757132, "City": "Cochin", "Gender": "Male", "DOB": "16-061981", "Fees": 4555000, "Experience": 9, "Address": "#401 Madiwala"}}),
    ("Dravid", {"personal_info": {"Age": 28, "Pin": 973132, "City": "Amaravati", "Gender": "Male", "DOB": "16-061981", "Fees": 6789000, "Experience": 12, "Address": "789 Mumbai"}}),
    ("Joseph", {"personal_info": {"Age": 30, "Pin": 678132, "City": "Mumbai", "Gender": "Male", "DOB": "16-061981", "Fees": 233000, "Experience": 3, "Address": "323 3rd Cross"}}),
    ("Dhanush", {"personal_info": {"Age": 25, "Pin": 874132, "City": "Delhi", "Gender": "Male", "DOB": "16-061981", "Fees": 9786000, "Experience": 10, "Address": "456 Maple Ave"}}),
    ("Sam", {"personal_info": {"Age": 28, "Pin": 632132, "City": "Ahmadabad", "Gender": "Male", "DOB": "16-061981", "Fees": 984567000, "Experience": 11, "Address": "189 Walaja"}})
]

# Create DataFrame
df_nest_map = spark.createDataFrame(data, schema=schema)

# Display the DataFrame
display(df_nest_map)

# COMMAND ----------

# DBTITLE 1,Multi Nested MapType
# Corrected schema definition
schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Profile", MapType(
        StringType(), 
        StructType([
            StructField("Name", StringType(), True),
            StructField("Age", IntegerType(), True),
            StructField("City", StringType(), True),
            StructField("Gender", StringType(), True),
            StructField("Skills", ArrayType(
                StructType([
                    StructField("SkillName", StringType(), True),
                    StructField("ExperienceYears", IntegerType(), True)
                ]), True), True),
            StructField("Matrix", ArrayType(
                StructType([
                    StructField("SkillName", StringType(), True),
                    StructField("ExperienceYears", IntegerType(), True)
                ]), True), True)
        ]), True), True)
])

# Sample data
data = [
    ("1", {"personal_info": {"Name": "John", "Age": 30, "City": "Bangalore", "Gender": "Male", "Skills": [{"SkillName": "Python", "ExperienceYears": 5}, {"SkillName": "Spark", "ExperienceYears": 3}], "Matrix": [{"SkillName": "Python", "ExperienceYears": 5}, {"SkillName": "Spark", "ExperienceYears": 3}]}}),
    ("2", {"personal_info": {"Name": "Kiran", "Age": 25, "City": "Hyderabad", "Gender": "Male", "Skills": [{"SkillName": "Java", "ExperienceYears": 4}, {"SkillName": "Kubernetes", "ExperienceYears": 6}], "Matrix": [{"SkillName": "Python", "ExperienceYears": 5}, {"SkillName": "Spark", "ExperienceYears": 3}]}}),
    ("3", {"personal_info": {"Name": "Kishore", "Age": 45, "City": "Chennai", "Gender": "Male", "Skills": [{"SkillName": "PySpark", "ExperienceYears": 6}, {"SkillName": "Spark", "ExperienceYears": 8}], "Matrix": [{"SkillName": "Python", "ExperienceYears": 5}, {"SkillName": "Spark", "ExperienceYears": 3}]}}),
    ("4", {"personal_info": {"Name": "Kashvi", "Age": 28, "City": "Nasik", "Gender": "Male", "Skills": [{"SkillName": "SQL", "ExperienceYears": 8}, {"SkillName": "Spark", "ExperienceYears": 5}], "Matrix": [{"SkillName": "Python", "ExperienceYears": 5}, {"SkillName": "Spark", "ExperienceYears": 3}]}}),
    ("5", {"personal_info": {"Name": "Kamal", "Age": 39, "City": "Mumbai", "Gender": "Male", "Skills": [{"SkillName": "Devops", "ExperienceYears": 2}, {"SkillName": "Spark", "ExperienceYears": 6}], "Matrix": [{"SkillName": "Python", "ExperienceYears": 5}, {"SkillName": "Spark", "ExperienceYears": 3}]}}),
    ("6", {"personal_info": {"Name": "Pratap", "Age": 49, "City": "Ahmadabad", "Gender": "Male", "Skills": [{"SkillName": "Databricks", "ExperienceYears": 7}, {"SkillName": "Spark", "ExperienceYears": 7}], "Matrix": [{"SkillName": "Python", "ExperienceYears": 5}, {"SkillName": "Spark", "ExperienceYears": 3}]}})
]

# Create DataFrame
df_multi_nest = spark.createDataFrame(data, schema=schema)

# Display the DataFrame
display(df_multi_nest)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Creating a DataFrame using create_map**

# COMMAND ----------

# Sample DataFrame with a StringType column containing JSON strings
data = [("Naresh", "Bangalore", 2, 21, 41, {"Age": 25, "emp_id": 768954, "Exp": 5}), 
        ("Harish", "Chennai", 4, 12, 5, {"Age": 30, "emp_id": 768956, "Exp": 2}),
        ("Prem", "Hyderabad", 5, 9, 12, {"Age": 28, "emp_id": 798954, "Exp": 8}), 
        ("Prabhav", "kochin", 7, 12, 4, {"Age": 35, "emp_id": 788956, "Exp": 6}),
        ("Hari", "Nasik", 8, 51, 35, {"Age": 21, "emp_id": 769954, "Exp": 9}), 
        ("Druv", "Delhi", 12, 15, 12, {"Age": 36, "emp_id": 768946, "Exp": 4}),
        ]

# Define the schema for the MapType column
map_schema = StructType([
  StructField("Name", StringType(), True),
  StructField("City", StringType(), True),
  StructField("key", IntegerType(), True),
  StructField("mode", IntegerType(), True),
  StructField("target", IntegerType(), True),
  StructField("Properties", MapType(StringType(), IntegerType()))])

# Convert the StringType column to a MapType column
df_map_nest = spark.createDataFrame(data, map_schema)

# Display the resulting DataFrame
display(df_map_nest)

# COMMAND ----------

df_nest = df_map_nest.select("*", f.array('key', 'mode', 'target').alias('audience'),
                             f.create_map(
                               f.lit('acousticness'), col('Name'), 
                               f.lit('danceability'), col('City'),
                               f.lit('energy'), col('City'),
                               f.lit('instrumentalness'), col('City'),
                               f.lit('liveness'), col('City'),
                               f.lit('loudness'), col('City'),
                               f.lit('speechiness'), col('City'),
                               f.lit('tempo'), col('Name')).alias('qualities'))

display(df_nest)
