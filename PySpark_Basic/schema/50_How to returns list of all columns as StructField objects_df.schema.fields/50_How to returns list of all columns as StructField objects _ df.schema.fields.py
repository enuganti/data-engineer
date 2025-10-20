# Databricks notebook source
# MAGIC %md
# MAGIC **How to return list of all columns as StructField objects?**

# COMMAND ----------

# MAGIC %md
# MAGIC | Code Part                                                    | Purpose                                    |
# MAGIC | ------------------------------------------------------------ | ------------------------------------------ |
# MAGIC | `df.schema`                                                  | Returns full schema object                 |
# MAGIC | `df.schema.fields`                                           | Returns **list of all columns** as **StructField** objects |
# MAGIC | `field.name`                                                 | Gets the **column name** from each **StructField** |
# MAGIC | `field.dataType`                                             | Column data type                           |
# MAGIC | `field.nullable`                                             | Whether nulls are allowed                  |
# MAGIC | `for field in df.schema.fields'                              | Iterate over all schema fields             |
# MAGIC | `any(field.name == "colname" for field in df.schema.fields)` | Check if column exists (Returns **True** if any field matches) |
# MAGIC | `[field.name for field in df.schema.fields]`                 | Extract list of column names               |

# COMMAND ----------

df = spark.read.csv("/Volumes/@azureadb/pyspark/unionby/company_level.csv", header=True, inferSchema=True)
display(df.limit(5))

# COMMAND ----------

# Display schema (structure of the DataFrame)
df.printSchema()

# COMMAND ----------

# Access all fields (columns) in schema
print("\n=== All Fields in Schema ===")
print(df.schema.fields)

# COMMAND ----------

for field in df.schema.fields:
    print(field)

# COMMAND ----------

StructField_names = [field for field in df.schema.fields]
print(StructField_names)

# COMMAND ----------

df.schema['start_date'].dataType

# COMMAND ----------

# Print column name, data type, and nullable for each field
print("\n=== Detailed Field Information ===")
for field in df.schema.fields:
    print(f"Column Name   : {field.name}")
    print(f"Data Type     : {field.dataType}")
    print(f"Nullable      : {field.nullable}")
    print("-" * 35)

# COMMAND ----------

# Extract only column names using a list comprehension
column_names = [field.name for field in df.schema.fields]
print("\nColumn Names:", column_names)

# COMMAND ----------

datatypes = [field.dataType for field in df.schema.fields]
print(datatypes)

# COMMAND ----------

fiels_nullable = [field.nullable for field in df.schema.fields]
print(fiels_nullable)

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Use df.schema.fields for Conditional Checks**

# COMMAND ----------

# Check if a specific column exists using df.schema.fields
if any(field.name == "department" for field in df.schema.fields):
    print("\n Column 'department' exists in DataFrame")
else:
    print("\n Column 'department' not found")

if any(field.name == "bonus" for field in df.schema.fields):
    print(" Column 'bonus' exists in DataFrame")
else:
    print(" Column 'bonus' not found")

if any(field.name == "category" for field in df.schema.fields):
    print(" Column 'category' exists in DataFrame")
else:
    print(" Column 'category' not found")

if any(field.name == "session_name" for field in df.schema.fields):
    print(" Column 'session_name' exists in DataFrame")
else:
    print(" Column 'session_name' not found")

if any(field.name == "product_id" for field in df.schema.fields):
    print(" Column 'product_id' exists in DataFrame")
else:
    print(" Column 'product_id' not found")

# COMMAND ----------

# MAGIC %md
# MAGIC       (field.name == "category" for field in df.schema.fields)
# MAGIC - This generator expression loops through **each field** in **df.schema.fields** and checks:
# MAGIC   - “Is this field’s name **equal** to **category**?”
# MAGIC   - It produces a **sequence of Boolean values**: **[False, True, False]**
# MAGIC   - if only one of the columns (like "category") matches.

# COMMAND ----------

# MAGIC %md
# MAGIC       if any(field.name == "category" for field in df.schema.fields):
# MAGIC - The built-in **any()** function returns:
# MAGIC   - **True** → if **at least one** of the conditions is **True**.
# MAGIC   - **False** → if **none** of the conditions is **True**.

# COMMAND ----------

# Create a list of column:datatype pairs
column_details = [(field.name, str(field.dataType)) for field in df.schema.fields]
print("\nColumn Details (ColumnName → DataType):", column_details)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Loop through Schema Fields and Display Details**

# COMMAND ----------

print("DataFrame Schema Details:\n")
print("-" * 50)
print("{:<22} {:<16} {:<10}".format("Column Name", "Data Type", "Nullable"))
print("-" * 50)

for field in df.schema.fields:
    print("{:<20} {:<20} {:<15}".format(field.name, str(field.dataType), str(field.nullable)))

print("x" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Access Specific Attributes**

# COMMAND ----------

# Example: Access the first field
field = df.schema.fields[0]

print("Column Name  :", field.name)
print("Data Type    :", field.dataType)
print("Nullable     :", field.nullable)

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Extract Schema Information into a Pandas Table**

# COMMAND ----------

import pandas as pd

schema_info = [(field.name, str(field.dataType), field.nullable) for field in df.schema.fields]
schema_df = pd.DataFrame(schema_info, columns=["Column Name", "Data Type", "Nullable"])

schema_df