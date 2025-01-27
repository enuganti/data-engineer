# Databricks notebook source
# MAGIC %md
# MAGIC #### **1) Applying random.choice to a Single Column**

# COMMAND ----------

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import random

# COMMAND ----------

# DBTITLE 1,sample dataframe
# Sample DataFrame
data = [(1, "Anand"),
        (2, "Baskar"),
        (3, "Catherin"),
        (4, "Dravid"),
        (5, "Swetha"),
        (6, "Akash"),
        (7, "Senthil"),
        (8, "Praveen")]
columns = ["ID", "Name"]

df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

# List of random choices
choices = ["HR", "IT", "Finance", "Marketing"]

# Define UDF to apply random.choice
@udf(StringType())
def random_choice_udf():
    return random.choice(choices)

# Apply the UDF to add a new column
df_with_random_choice = df.withColumn("Department", random_choice_udf())

# Show the resulting DataFrame
display(df_with_random_choice)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Applying random.choice to Multiple Columns**

# COMMAND ----------

# DBTITLE 1,udf on multiple columns
# List of random choices
choices = ["HR", "IT", "Finance", "Marketing"]

# Define UDF for random department
@udf(StringType())
def random_department():
    return random.choice(choices)

# Another UDF for random project assignment
projects = ["Project A", "Project B", "Project C"]

@udf(StringType())
def random_project():
    return random.choice(projects)

# Apply both UDFs to add new columns
df_with_random_columns = (
    df.withColumn("Department", random_department())
      .withColumn("Project", random_project())
)

# Show the resulting DataFrame
display(df_with_random_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **3) Using a Predefined List in random.choice with Existing Column Data**

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      dict.get(key,value)
# MAGIC
# MAGIC **key:** (Required) Key to be **searched in the dictionary**.
# MAGIC
# MAGIC **value:** (Optional) Value to be returned if the **key is not present** in the dictionary.

# COMMAND ----------

course = {'language': 'python', 'fee': 4000}

# Using get() method to get the value from dictionary
print('language:', course.get('language'))
print('fee:', course.get('fee'))

# COMMAND ----------

# Using get() to get the value as a None
print('duration:', course.get('duration'))

# COMMAND ----------

# Using get() to get the value as specified
print('duration:', course.get('duration','Not in dictionary'))

# COMMAND ----------


# Using get() to get the value as specified
course = {'language': 'python', 'fee': 4000}
print('duration:', course.get('language','Not in dictionary'))

# COMMAND ----------

# Using get() to get the value as specified
course = {'language': 'python', 'fee': 4000, 'HR': ["Policy", "Recruitment", "Sales"], 'Finance': ["Auditing", "Budgeting", "Works", "Temp"]}

print('HR:', course.get('HR','Not in dictionary'))
print('Random Choice of HR:', random.choice(course.get('HR','Not in dictionary')))

print('Finance:', course.get('Finance','Not in dictionary'))
print('Random Choice of Finance:', random.choice(course.get('Finance','Not in dictionary')))

# COMMAND ----------

@udf(StringType())
def random_from_column(value):
    options = {
        "HR": ["Policy"],
        "IT": ["DevOps"],
        "Finance": ["Auditing"]
    }
    return random.choice(options.get(value, ["General"]))

# Add a new column with predefined random values based on "Department"
df_with_dependent_choice = df_with_random_columns.withColumn("Specialization", random_from_column(col("Department")))

display(df_with_dependent_choice)
