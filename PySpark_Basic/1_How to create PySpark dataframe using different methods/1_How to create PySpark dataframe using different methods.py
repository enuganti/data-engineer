# Databricks notebook source
# MAGIC %md
# MAGIC **Topics covered**
# MAGIC
# MAGIC      1) Create a DataFrame from a List of Tuples
# MAGIC      2) Create a DataFrame from a List of Lists
# MAGIC      3) Create a DataFrame using dictionary
# MAGIC      4) Create a DataFrame from a Simple List
# MAGIC      5) Create a DataFrame with an Explicit Schema
# MAGIC      6) Create a DataFrame Directly from a List Using Row
# MAGIC      7) toDF()

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Create a DataFrame from a List of Tuples**
# MAGIC - If your **list** contains **tuples** where **each tuple represents a row**, you can create a DataFrame

# COMMAND ----------

# List of tuples
data = [(1, "Albert", 25, "Sales", "ADF"),
        (2, "Buvan", 30, "Marketing", "Oracle"),
        (3, "Chandar", 28, "IT", "SAP"),
        (4, "Syam", 33, "Admin", "Tally"),
        (5, "Senthil", 26, "Production", "MATLAB"),
        (6, "Surya", 35, "Quality", "Excel")]

# Define column names
columns = ["ID", "Name", "Age", "Department", "Technology"]

# COMMAND ----------

# DBTITLE 1,Method 01
# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Display DataFrame
display(df)

# COMMAND ----------

# DBTITLE 1,Method 02
# Create DataFrame
df1 = spark.createDataFrame(data, 'ID int, Name string, Age int, Department string, Technology string')

# Display DataFrame
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Create a DataFrame from a List of Lists**
# MAGIC - If your **list** contains **lists instead of tuples**.

# COMMAND ----------

# List of Lists
data = [[1, "Albert", 25, "Sales", "ADF"],
        [2, "Buvan", 30, "Marketing", "Oracle"],
        [3, "Chandar", 28, "IT", "SAP"],
        [4, "Syam", 33, "Admin", "Tally"]]

# Define column names
columns = ["ID", "Name", "Age", "Department", "Technology"]

df2 = spark.createDataFrame(data, schema=columns)
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Create a DataFrame using dictionary**

# COMMAND ----------

data = [{'Name':'Jayanth', 'ID':'A123', 'Country':'USA'},
        {'Name':'Rupesh', 'ID':'A124', 'Country':'USA'},
        {'Name':'Thrusanth', 'ID':'A125', 'Country':'IND'},
        {'Name':'Jahangeer', 'ID':'A126', 'Country':'USA'},
        {'Name':'Sowmya', 'ID':'A127', 'Country':'INA'}]

df_dict = spark.createDataFrame(data)
display(df_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Create a DataFrame from a Simple List**
# MAGIC - If your **list** contains a **single column**, you can still use createDataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC      # Method 01
# MAGIC      df = spark.createDataFrame(data, 'int')
# MAGIC
# MAGIC      # Method 02
# MAGIC      df = spark.createDataFrame([(x,) for x in data], ["Numbers"])

# COMMAND ----------

# DBTITLE 1,Method 01
data = [15, 25, 36, 44, 57, 65, 89, 95, 9]

df3 = spark.createDataFrame(data, 'int')
display(df3)

# COMMAND ----------

df3 = df3.withColumnRenamed("value", "Numbers")
display(df3)

# COMMAND ----------

# DBTITLE 1,Method 02
data = [15, 25, 36, 44, 57, 65, 89, 95, 9]

df4 = spark.createDataFrame([(x,) for x in data], ["Numbers"])
display(df4)

# COMMAND ----------

# DBTITLE 1,Method 03
# Create a sample DataFrame
data = [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)]

df41 = spark.createDataFrame(data, ["id"])
display(df41)

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Create a DataFrame with an Explicit Schema**
# MAGIC - You can define the **schema** explicitly using **StructType and StructField**.

# COMMAND ----------

# DBTITLE 1,Method 01
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

data = [[1, "Albert", 25, "Sales", "ADF"],
        [2, "Buvan", 30, "Marketing", "Oracle"],
        [3, "Chandar", 28, "IT", "SAP"],
        [4, "Syam", 33, "Admin", "Tally"],
        [5, "Bharat", 28, "Maintenance", "Excel"]]

schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Department", StringType(), True),
    StructField("Technology", StringType(), True)
])

df_schema = spark.createDataFrame(data, schema=schema)
display(df_schema)

# COMMAND ----------

# DBTITLE 1,Method 02
# create student data with 5 rows and 6 attributes
students =[['001', 'sravan', 23, 5.79, 67, 'Chennai'],
            ['002', 'ojaswi', 16, 3.79, 34, 'Hyderabad'],
            ['003', 'gnanesh chowdary', 7, 2.79, 17, 'Bangalore'],
            ['004', 'rohith', 9, 3.69, 28, 'Delhi'],
            ['005', 'sridevi', 37, 5.59, 54, 'Nasik']]

# define the StructType and StructFields for the below column names
schema = """rollno string, name string, age int, height float, weight int, address string"""

# create the dataframe and add schema to the dataframe
df_schema_string = spark.createDataFrame(students, schema=schema)
display(df_schema_string)

# COMMAND ----------

# DBTITLE 1,Method 03
data = [(1, 'Sandhya', [20, 30, 40]),
        (2, 'Alex', [40, 20, 10]),
        (3, 'Joseph', []),
        (4, 'Arya', [20, 1, None])]

df_schema_def = spark.createDataFrame(data, schema="ID int, Name string, Marks array<int>")
display(df_schema_def)

# COMMAND ----------

# MAGIC %md
# MAGIC **6) Create a DataFrame Directly from a List Using Row**

# COMMAND ----------

from pyspark.sql import Row

data = [Row(ID=1, Name="Albert", Age=25, Department="Sales", Technology="ADF"),
        Row(ID=2, Name="Buvan", Age=30, Department="Marketing", Technology="Oracle"),
        Row(ID=3, Name="Chandar", Age=28, Department="IT", Technology="SAP"),
        Row(ID=4, Name="Syam", Age=33, Department="Admin", Technology="Tally"),
        Row(ID=5, Name="Bharat", Age=28, Department="Maintenance", Technology="Excel")]

df_row = spark.createDataFrame(data)
display(df_row)

# COMMAND ----------

# MAGIC %md
# MAGIC **7) toDF()**

# COMMAND ----------

employees = [(1, "Santhosh", "Kumar", 1000.0, "united states", "+1 123 456 7890", "123 45 6789"),
             (2, "Hemanth", "Raju", 1250.0, "India", "+91 234 567 8901", "456 78 9123"),
             (3, "Nisha", "Kakkar", 750.0, "united KINGDOM", "+44 111 111 1111", "222 33 4444"),
             (4, "Bobby", "Deol", 1500.0, "AUSTRALIA", "+61 987 654 3210", "789 12 6118"),
             (5, "Harish", "Rao", 1650.0, "Sweden", "+91 234 567 8901", "456 78 9123"),
             (6, "Yung", "Lee", 850.0, "China", "+44 222 111 1111", "567 33 4444"),
             (7, "Bushan", "Dayal", 1770.0, "Butan", "+71 932 654 5215", "489 16 8318")]

# create the dataframe
df_todf = spark.createDataFrame(employees). \
     toDF("employee_id", "first_name", "last_name", "salary", "nationality", "phone_number", "ssn")
display(df_todf)