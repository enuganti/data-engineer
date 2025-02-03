# Databricks notebook source
# MAGIC %md
# MAGIC #### **Question 01**

# COMMAND ----------

# MAGIC %md
# MAGIC      Q1) How to find null rows in columns using pyspark?
# MAGIC      
# MAGIC      Q2) How do you clean below data using databricks. Handling blank and null?
# MAGIC
# MAGIC              name         email        phoneNo   country
# MAGIC              xyz          blank         1234      null
# MAGIC              null    abs@gmail.com      blank     blank
# MAGIC              abc          null          null      india
# MAGIC              def     xyz@gmail.com      5678      sweden
# MAGIC              None         None          None      None

# COMMAND ----------

# DBTITLE 1,Create Table
 # Sample data
 data = [
     ("xyz", "blank", "1234", None),
     (None, "abs@gmail.com", "blank", "blank"),
     ("abc", None, None, "india"),
     ("def", "xyz@gmail.com", "5678", "sweden"),
     (None, None, None, None)
 ]

 # Define schema
 schema = ["name", "email", "phoneNo", "country"]

 # Create DataFrame
 df = spark.createDataFrame(data, schema)
 display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Q1) How to find null rows in columns using PySpark?**

# COMMAND ----------

# DBTITLE 1,or
from pyspark.sql.functions import col

# Find rows where any column has NULL values
df_nulls = df.filter(col("name").isNull() | col("email").isNull() | col("phoneNo").isNull() | col("country").isNull())

display(df_nulls)

# COMMAND ----------

# DBTITLE 1,and
# Find rows where any column has NULL values
df_nulls_all = df.filter(col("name").isNull() & col("email").isNull() & col("phoneNo").isNull() & col("country").isNull())

display(df_nulls_all)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Q2) How to clean the data in Databricks (Handling Blank & Null Values)?**

# COMMAND ----------

# MAGIC %md
# MAGIC       # Replacing Missing Values with new values
# MAGIC       df.na.replace(old_value, new_vallue)

# COMMAND ----------

# MAGIC %md
# MAGIC      # Fill NULL values for each column based on the requirement
# MAGIC      df_cleaned = df_cleaned.fillna({
# MAGIC          "name": "vowel",
# MAGIC          "email": "no_email@domain.com",
# MAGIC          "phoneNo": "9876",  
# MAGIC          "country": "sweden"
# MAGIC      })
# MAGIC                         (or)
# MAGIC      # Fill NULL values for each column based on the requirement
# MAGIC      df_cleaned = df_cleaned.na.fill({
# MAGIC          "name": "vowel",
# MAGIC          "email": "no_email@domain.com",
# MAGIC          "phoneNo": "9876",  
# MAGIC          "country": "sweden"
# MAGIC      })

# COMMAND ----------

# Replace 'blank' with None and then fill None with desired values
df_blank = df.replace("blank", None)
display(df_blank)

# COMMAND ----------

cleaned_df = df_blank.na.fill({
                  "name": "vowel",
                  "email": "noemail@example.com",
                  "phoneNo": "9876",
                  "country": "sweden"})

# Display the cleaned DataFrame
display(cleaned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 02**

# COMMAND ----------

# MAGIC %md
# MAGIC      json_data = '''
# MAGIC      {
# MAGIC        "id": 1,
# MAGIC        "name": "John",
# MAGIC        "details": {
# MAGIC          "age": 25,
# MAGIC          "gender": "Male",
# MAGIC          "address": {
# MAGIC            "street": "123 Main St",
# MAGIC            "city": "Anytown",
# MAGIC            "state": "CA",
# MAGIC            "zip": "12345"
# MAGIC          },
# MAGIC          "contacts": {
# MAGIC            "email": "john@example.com",
# MAGIC            "phone": "+1-555-123-4567"
# MAGIC          }
# MAGIC        }
# MAGIC      }
# MAGIC      '''
# MAGIC
# MAGIC      i) How to read above json using pyspark?
# MAGIC      ii) How to get the `email` from `contacts` from above json using same code?

# COMMAND ----------

# Sample JSON data
json_data = '''
{
  "id": 1,
  "name": "John",
  "details": {
    "age": 25,
    "gender": "Male",
    "address": {
      "street": "123 Main St",
      "city": "Anytown",
      "state": "CA",
      "zip": "12345"
    },
    "contacts": {
      "email": "john@example.com",
      "phone": "+1-555-123-4567"
    }
  }
}
'''

dbutils.fs.put('dbfs:/FileStore/tables/InterviewQuestions/PySpark/json_data.json', json_data, True)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/InterviewQuestions/PySpark/

# COMMAND ----------

# MAGIC %md
# MAGIC #### **a) How to read above json using pyspark?**

# COMMAND ----------

# df_json = spark.read.json("dbfs:/FileStore/tables/InterviewQuestions/PySpark/json_data.json", multiLine=True)
df_json = spark.read.format('json')\
                    .option('header', True)\
                    .option('inferSchema', True)\
                    .option('multiLine', True)\
                    .load("dbfs:/FileStore/tables/InterviewQuestions/PySpark/json_data.json")
display(df_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **b) How to get the `email` from `contacts` from above json using same code?**

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Extract the email from contacts
email_df = df_json.select(col("details.contacts.email").alias("email"))

# Display the email DataFrame
display(email_df)
