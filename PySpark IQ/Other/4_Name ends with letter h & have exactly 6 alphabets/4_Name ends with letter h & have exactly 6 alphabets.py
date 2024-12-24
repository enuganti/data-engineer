# Databricks notebook source
# MAGIC %md
# MAGIC **Problem Statement:**
# MAGIC
# MAGIC Write a PySpark script to find the details of students whose **first names end with the letter ‘h’** and have **exactly six alphabets**.

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import col, upper, length, substring

# COMMAND ----------

# Sample data
data = [(1, 'Ramesh', 'Babu', 3.3, '2024-07-01', 'Commerce'),
        (2, 'Sharath', 'Kumar', 3.9, '2022-07-01', 'AI'),
        (3, 'Venkatesh', 'Raj', 3.2, '2023-07-20', 'MBBS'),
        (4, 'Joseph', 'Brown', 3.7, '2023-07-01', 'Aerospace'),
        (5, 'Sourabh', 'saxena', 3.4, '2022-06-15', 'Engineering'),
        (6, 'Leah', 'Smith', 3.7, '2021-07-22', 'Masters'),
        (7, 'Prakash', 'Raja', 3.8, '2022-07-05', 'Scientist'),        
        (8, 'Rakesh', 'Kumar', 3.9, '2020-07-01', 'Data Scientist'),       
        (9, 'Kamalesh', 'Rao', 3.6, '2023-07-01', 'Social'),
        (10, 'Kamath', 'mohan', 3.7, '2023-07-01', 'Science'),
        (11, 'Naagesh', 'Babu', 3.5, '2024-07-01', 'Commerce'),
        (12, 'Praharsh', 'Mohan', 3.4, '2023-07-01', 'Science'),
        (13, 'Danush', 'Vedanth', 3.3, '2021-07-01', 'Social'),
        (14, 'Kanniah', 'kanth', 3.2, '2023-07-01', 'Masters')
       ]

# Define schema
columns = ["student_id", "first_name", "last_name", "gpa", "enrollment_date", "major"]

# Create DataFrame
students_df = spark.createDataFrame(data, schema=columns)

# Show DataFrame
display(students_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution**

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**

# COMMAND ----------

# Filter DataFrame
filtered_students_01 = students_df.filter((col("first_name").rlike(r'^[A-Za-z]{5}h$')))

# Show filtered DataFrame
display(filtered_students_01)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 02**

# COMMAND ----------

# Filter DataFrame
filtered_students_02 = students_df.filter((col('first_name').endswith('h')) & (length(col('first_name')) == 6))

# Show filtered DataFrame
display(filtered_students_02)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 03**

# COMMAND ----------

# Filter DataFrame
from pyspark.sql.functions import upper, substring, length, col

filtered_students_03 = students_df.where(
    (upper(substring(col('first_name'), -1, 1)) == 'H') & (length(col('first_name')) == 6)
)

# Show filtered DataFrame
display(filtered_students_03)
