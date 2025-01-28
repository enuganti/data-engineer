# Databricks notebook source
df = spark.read.csv("/FileStore/tables/random_data-3.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

display(df.select('Company_Name').distinct())
display(df.select('Category').distinct())
display(df.select('Cust_Type').distinct())
display(df.select('Exchange').distinct())
display(df.select('Location').distinct())
display(df.select('Cust_Category').distinct())
display(df.select('Index').distinct())

# COMMAND ----------

import random
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, rand, array, element_at, lit
from pyspark.sql.types import StringType, BooleanType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Method 01**

# COMMAND ----------

# Extract distinct values for Cust_Name from the DataFrame
Cust_Name_list = [row['Cust_Name'] for row in df.select('Cust_Name').distinct().collect()]

# Define UDFs for replacing column values with random choices
@udf(StringType())
def random_company_name():
    return random.choice(["Sony", "BP", "Philips", "BPL", "Reliance"])

@udf(StringType())
def random_cust_name():
    return random.choice(Cust_Name_list)

@udf(StringType())
def random_category():
    return random.choice(["Premium", "Lower", "Medium", "Upper", "Standard"])

@udf(StringType())
def random_cust_type():
    return random.choice(["Standard", "Premium", "Luxury"])

@udf(StringType())
def random_exchange():
    return random.choice(["EURO", "INR", "DOLLOR", "SEK"])

@udf(StringType())
def random_location():
    return random.choice(["SriLanka", "USA", "INDIA", "UK", "SWEDEN", "GERMANY"])

@udf(StringType())
def random_cust_category():
    return random.choice(["TOI", "RTO", "SETTL", "ADB", "TRADING"])

@udf(BooleanType())
def random_boolean():
    return random.choice([True, False])

@udf(IntegerType())
def fixed_zero():
    return 0

@udf(IntegerType())
def fixed_one():
    return 1

@udf(StringType())
def empty_string():
    return ""

# Replace columns with random values
df_with_random_values = (
    df.withColumn("Company_Name", random_company_name())
      .withColumn("Cust_Name", random_cust_name())
      .withColumn("Category", random_category())
      .withColumn("Cust_Type", random_cust_type())
      .withColumn("Exchange", random_exchange())
      .withColumn("Location", random_location())
      .withColumn("Cust_Category", random_cust_category())
      .withColumn("Index", random_boolean())
      .withColumn("impact1", fixed_zero())
      .withColumn("impact2", fixed_one())
      .withColumn("impact3", empty_string())
)

# Display the updated DataFrame
display(df_with_random_values)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Method 02**

# COMMAND ----------

# Define fixed lists for random selection
company_names = ["Sony", "BP", "Philips", "BPL", "Reliance"]
categories = ["Premium", "Lower", "Medium", "Upper", "Standard"]
cust_types = ["Standard", "Premium", "Luxury"]
exchanges = ["EURO", "INR", "DOLLOR", "SEK"]
locations = ["SriLanka", "USA", "INDIA", "UK", "SWEDEN", "GERMANY"]
cust_categories = ["TOI", "RTO", "SETTL", "ADB", "TRADING"]

# Create arrays for random selection
df_with_random_values = (
    df.withColumn("Company_Name", element_at(array([lit(x) for x in company_names]), (rand() * len(company_names) + 1).cast("int")))
      .withColumn("Category", element_at(array([lit(x) for x in categories]), (rand() * len(categories) + 1).cast("int")))
      .withColumn("Cust_Type", element_at(array([lit(x) for x in cust_types]), (rand() * len(cust_types) + 1).cast("int")))
      .withColumn("Exchange", element_at(array([lit(x) for x in exchanges]), (rand() * len(exchanges) + 1).cast("int")))
      .withColumn("Location", element_at(array([lit(x) for x in locations]), (rand() * len(locations) + 1).cast("int")))
      .withColumn("Cust_Category", element_at(array([lit(x) for x in cust_categories]), (rand() * len(cust_categories) + 1).cast("int")))
      .withColumn("Index", (rand() > 0.5).cast("boolean"))  # Random boolean
      .withColumn("impact1", lit(0))  # Fixed value 0
      .withColumn("impact2", lit(1))  # Fixed value 1
      .withColumn("impact3", lit(""))  # Empty string
)

# Display the updated DataFrame
display(df_with_random_values)

# COMMAND ----------

df1 = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ['id'])
display(df1)

# COMMAND ----------

df1.withColumn('new', (rand() * len(categories) + 1).cast("int")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **element_at**
# MAGIC
# MAGIC - PySpark SQL Functions **element_at** method is used to extract values from **lists or maps** in a PySpark Column.
# MAGIC
# MAGIC **Syntax**
# MAGIC
# MAGIC      element_at(arrayExpr, index)
# MAGIC      element_at(mapExpr, key)

# COMMAND ----------

# DBTITLE 1,List
rows = [[[5,6]], [[7,8]], [[8,4]], [[6,9]], [[2,9]]]

df_element = spark.createDataFrame(rows, ['List'])
display(df_element)

# COMMAND ----------

df_res = df_element.withColumn('1st value', F.element_at('List',1))\
                   .withColumn('2nd value', F.element_at('List',2))\
                   .withColumn('3rd value', F.element_at('List',3))\
                   .withColumn('-1st value', F.element_at('List',-1))\
                   .withColumn('-2nd value', F.element_at('List',-2))\
                   .withColumn('-3rd value', F.element_at('List',-3))
display(df_res)

# COMMAND ----------

# DBTITLE 1,map
rows = [[{'A':4}], [{'A':3}], [{'B':4}], [{'A':5, 'B':6}], [{'A':8, 'B':9}]]

df_map = spark.createDataFrame(rows, ['map'])
display(df_map)

# COMMAND ----------

df_map = df_map.select(F.element_at('map', F.lit('A')))
display(df_map)
