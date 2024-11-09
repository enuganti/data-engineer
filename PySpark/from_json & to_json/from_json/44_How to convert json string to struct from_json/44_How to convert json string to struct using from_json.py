# Databricks notebook source
# MAGIC %md
# MAGIC #### **from_json**
# MAGIC
# MAGIC - Function is used to convert **JSON string** into **Struct type or Map type**.
# MAGIC - If the **string** is **unparseable**, it returns **null**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax**
# MAGIC
# MAGIC - **col:** Name of column that contains the **json string**.
# MAGIC - **schema:** a **StructType or ArrayType of StructType** to use when parsing the json column.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **How to convert JSON string to StructType?**

# COMMAND ----------

# MAGIC %md
# MAGIC 1) JSON String
# MAGIC 2) Nested Structure
# MAGIC 3) CSV to StructType to JSON string to StructType

# COMMAND ----------

# MAGIC %md
# MAGIC **1) JSON String**

# COMMAND ----------

from pyspark.sql.functions import from_json, lit, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 01**

# COMMAND ----------

data = [(123, '''{"Name":"Kiran", "Dept": "Admin", "Tech": "SAP", "age": 32, "Level": "High"}''', "Sony"),
        (124, '''{"Name":"kishore", "Dept": "IT", "Tech": "ORACLE", "age": 42, "Level": "Medium"}''', "kumar"),
        (125, '''{"Name":"karan", "Dept": "Testing", "Tech": "SQL", "age": 38, "Level": "Low"}''', "sharma"),
        (126, '''{"Name":"sharath", "Dept": "Prod", "Tech": "ANALYSIS", "age": 40, "Level": "Medium"}''', "roy"),
        (127, '''{"Name":"sayan", "Dept": "Sales", "Tech": "DATA SCIENCE", "age": 45, "Level": "Top"}''', "gupta")]

schema = ("id", "Format", "Name")

df_ex01 = spark.createDataFrame(data, schema)
display(df_ex01)

# COMMAND ----------

schema = StructType([StructField("Name", StringType(), True),
                     StructField("Dept", StringType(), True),
                     StructField("Tech", StringType(), True),
                     StructField("age", IntegerType(), True),
                     StructField("Level", StringType(), True)
                     ]
                    )

# COMMAND ----------

df_ex01 = df_ex01.select('Format', from_json(f.col('Format'), schema).alias("parsed_json"))
display(df_ex01)

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 02**

# COMMAND ----------

from pyspark.sql import SparkSession

# Assuming `dff` is your JSON string
dff = """[
    {"first_name": "kiran", "age": 32, "address": '{"city": "Baroda", "country": "india", "state": "Rj"}'},
    {"first_name": "Micheal", "age": 25, "address": '{"city": "Nasik", "country": "india", "state": "UP"}'},
    {"first_name": "Prakash", "age": 32, "address": '{"city": "Hyderabad", "country": "india", "state": "TS"}'},
    {"first_name": "Ritesh", "age": 25, "address": '{"city": "Bangalore", "country": "india", "state": "KA"}'},
    {"first_name": "Vaibhav", "age": 32, "address": '{"city": "Delhi", "country": "india", "state": "DL"}'},
    {"first_name": "Goerge", "age": 25, "address": '{"city": "Chennai", "country": "india", "state": "TN"}'}        
    ]"""

# Convert the JSON string to an RDD
rdd = spark.sparkContext.parallelize([dff])

# Read the JSON from the RDD
df_ex02 = spark.read.json(rdd, multiLine=True)

display(df_ex02)

# COMMAND ----------

schema1 = StructType([StructField("city", StringType(), True),
                      StructField("country", StringType(), True),
                      StructField("state", StringType(), True)
                      ]
                     )

# COMMAND ----------

df_ex02 = df_ex02.select('address', f.from_json(f.col('address'), schema1).alias("address_json"))
display(df_ex02)

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 03**

# COMMAND ----------

data = [("{\"city\":\"Baroda\", \"country\":\"india\", \"state\":\"RJ\"}",),
        ("{\"city\":\"Nasik\", \"country\":\"india\", \"state\":\"UP\"}",),
        ("{\"city\":\"Hyderabad\", \"country\":\"india\", \"state\":\"TS\"}",),
        ("{\"city\":\"Bangalore\", \"country\":\"india\", \"state\":\"KA\"}",),
         ]

df_ex03 = spark.createDataFrame(data, ["json_data"])
display(df_ex03)

# COMMAND ----------

schema2 = StructType([StructField("city", StringType(), True),
                      StructField("country", StringType(), True),
                      StructField("state", StringType(), True)
                      ]
                     )

# COMMAND ----------

df_ex03 = df_ex03.select('json_data', f.from_json(f.col('json_data'), schema2).alias("json_data_json"))
display(df_ex03)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Nested Structure**

# COMMAND ----------

# Sample data showcasing varying JSON structures
data = [("1", '{"name": "John Doe", "age": 30, "country": "USA", "First_Name": "Rakesh", "Last_Name": "Kumar"}'),
        ("2", '{"city": "New York", "country": "USA", "zipcode": "10001", "Tech": {"domain": "database", "designation": "data engineer", "Exp": 6, "Platform": "Azure"}}'),
        ("3", '{"product": "Laptop", "brand": "Dell", "specs": {"RAM": "16GB", "Storage": "512GB SSD", "OS": "windows"}}'),
        ("4", '{"First_Name": "Rakesh", "country": "IND", "zipcode": "560103", "specs": {"RAM": "16GB", "Storage": "512GB SSD", "OS": "windows"}, "Tech": {"domain": "database", "designation": "data engineer"}}'),
        ("5", '{"Last_Name": "Kumar", "country": "UK", "zipcode": "571323", "city": "SWEDEN", "age": 30, , "Tech": {"domain": "database", "designation": "data engineer"}}'),
        ("6", '{"name": "paul", "age": 35, "city": "SWEDEN", "country": "UK", "zipcode": "571323", "First_Name": "Sourabh", "Last_Name": "Kumar", "product": "Desktop", "brand": "HP", "specs": {"RAM": "32GB", "Storage": "512GB SSD", "OS": "windows"}}')
        ]

# Creating the DataFrame
df_ex02 = spark.createDataFrame(data, ["id", "json_string"])
display(df_ex02)

# COMMAND ----------

# Define the schema for the JSON data
schema = StructType([StructField("name", StringType(), True),
                     StructField("age", IntegerType(), True),
                     StructField("city", StringType(), True),
                     StructField("country", StringType(), True),
                     StructField("zipcode", StringType(), True),
                     StructField("product", StringType(), True),
                     StructField("brand", StringType(), True),
                     StructField("specs", StructType([StructField("RAM", StringType(), True),
                                                      StructField("Storage", StringType(), True),
                                                      StructField("OS", StringType(), True)
                                                      ])
                                 ),
                    StructField("Tech", StructType([StructField("domain", StringType(), True),
                                                    StructField("designation", StringType(), True),
                                                    StructField("Exp", IntegerType(), True),
                                                    StructField("Platform", StringType(), True)
                                                    ])
                                ),
                     StructField("First_Name", StringType(), True),
                     StructField("Last_Name", StringType(), True)
                     ])

# Apply the from_json function to convert the JSON string column to a struct type
df_ex03 = df_ex02.withColumn("json_struct", from_json(df_ex02["json_string"], schema))

# Display the resulting DataFrame
display(df_ex03)

# COMMAND ----------

# MAGIC %md
# MAGIC **3) CSV to StructType to JSON string to StructType**

# COMMAND ----------

df_json = spark.read.csv("dbfs:/FileStore/tables/to_json.csv", header=True, inferSchema=True)
display(df_json.limit(10))

# COMMAND ----------

# Select all columns and structure them into a single column named 'pp_msg'
df_stru = df_json.select(f.struct('*').alias('sales_msg')).distinct()
display(df_stru.limit(10))

# COMMAND ----------

# Convert the 'sales_msg' column to JSON string
df_final = df_stru.withColumn('sales_msg_json', f.to_json(f.col('sales_msg')))
display(df_final.limit(10))

# COMMAND ----------

schema = StructType([StructField('Id', IntegerType(), False),
                     StructField('Nick_Name', StringType(), False),
                     StructField('First_Name', StringType(), False),
                     StructField('Last_Name', StringType(), False),
                     StructField('Age', IntegerType(), False),
                     StructField('Type', StringType(), False),
                     StructField('Description', StringType(), False),
                     StructField('Commodity_Index', StringType(), False),
                     StructField('Sensex_Category', StringType(), False),
                     StructField('Label_Type', StringType(), False),
                     StructField('Effective_Date', StringType(), False),
                     StructField('Start_Date', StringType(), False)]
                    )

# COMMAND ----------

# Apply the from_json function on the JSON string column
df_final = df_final.select(f.from_json(f.col('sales_msg_json'), schema).alias('kafka_msg'))
display(df_final.limit(10))
