# Databricks notebook source
# MAGIC %md
# MAGIC #### UNIT TEST
# MAGIC - **How to check if column is available in schema or not?**

# COMMAND ----------

df = spark.read.csv("/Volumes/@azureadb/pyspark/unionby/company_level.csv", header=True, inferSchema=True)
display(df.limit(5))
print("List of Columns in df: ", df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Check if a single column exists using in Operator?**

# COMMAND ----------

display('cloud_flatform' in df.columns)

# COMMAND ----------

display('region' in df.columns)

# COMMAND ----------

display('country' in df.columns)

# COMMAND ----------

display('sales_organisation_id' in df.columns)

# COMMAND ----------

if "source_target" in df.columns:
    print("\nColumn 'source_target' is available in df")
else:
    print("\nColumn 'source_target' is not available in df")

# COMMAND ----------

if "sales_id" in df.columns:
    print("\nColumn 'sales_id' is available in df")
else:
    print("\nColumn 'sales_id' is not available in df")

# COMMAND ----------

# MAGIC %md
# MAGIC - The simplest method is using the **in** operator on **df.columns**.
# MAGIC - **df.columns** returns a **list of column** names (e.g. **['region', 'country', 'sales_organisation_id']**)
# MAGIC - The **in** operator checks if your **column name** is in that **list**.

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Check if multiple columns exist**

# COMMAND ----------

required_cols = ["region", "country", "sales_organisation_id"]

columns_exist = all(col in df.columns for col in required_cols)
display(columns_exist)

# COMMAND ----------

# MAGIC %md
# MAGIC - **col in df.columns** → returns **True** if that column **exists**, otherwise **False**.
# MAGIC - **all()** → returns **True** only **if all the conditions** inside are **True**.
# MAGIC - ✅ **columns_exist = True** → if **all required columns exist** in the DataFrame.
# MAGIC - ❌ **columns_exist = False** → if **any one** of them is **missing**.

# COMMAND ----------

required_cols = ["product_url", "product_id", "session_name"]

if all(col in df.columns for col in required_cols):
    print("All required columns exist in DataFrame")
else:
    missing = [col for col in required_cols if col not in df.columns]
    print(f"Missing columns: {missing}")

# COMMAND ----------

required_cols = ["region", "country", "sales_organisation_id"]

if all(col in df.columns for col in required_cols):
    print("All required columns exist in DataFrame")
else:
    missing = [col for col in required_cols if col not in df.columns]
    print(f"Missing columns: {missing}")

# COMMAND ----------

required_cols = ["region", "country", "session_name"]

if all(col in df.columns for col in required_cols):
    print("All required columns exist in DataFrame")
else:
    missing = [col for col in required_cols if col not in df.columns]
    print(f"Missing columns: {missing}")

# COMMAND ----------

# MAGIC %md
# MAGIC      if all(col in df.columns for col in required_cols):
# MAGIC - **df.columns** returns a **list of all column names** in the PySpark DataFrame df.
# MAGIC
# MAGIC       Example: ["region", "country", "sales_organisation_id", "sales_amount"]
# MAGIC
# MAGIC - **col in df.columns** checks whether **each column name** from **required_cols** is present in that **list**.
# MAGIC - The **all()** function returns:
# MAGIC   - **True:** if every required **column** is **present**.
# MAGIC   - **False:** if any **column** is **missing**.

# COMMAND ----------

# MAGIC %md
# MAGIC      else:
# MAGIC          missing = [col for col in required_cols if col not in df.columns]
# MAGIC          print(f"Missing columns: {missing}")
# MAGIC
# MAGIC - Create a **new list** called **missing**.
# MAGIC - It **collects all columns** from **required_cols** that are **not found in df.columns**.
# MAGIC

# COMMAND ----------

columns_to_check = ['ObjectID', 'ID', 'Name', 'Date', 'contract_source', 'currency_code', 'refunded_amount', 'start_date', 'product_url', 'category', 'default_group', 'source_target', 'cloud_flatform', 'session_id', 'session_name', 'status_name', 'status_type', 'sessions', 'product_id', 'load datetime']

# List all columns from columns_to_check that are NOT present in df.columns
[col for col in columns_to_check if col not in df.columns]

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Using DataFrame schema (if you need to inspect types)**
# MAGIC - If you also want to verify **column names** and their **data types**, use **df.schema.fields**.
# MAGIC - This is useful when you’re working with **nested or complex schemas**.

# COMMAND ----------

df.schema.fields

# COMMAND ----------

if any(field.name == "category" for field in df.schema.fields):
    print("\nColumn 'category' is available in df.schema.fields")
else:
    print("\nColumn 'category' is not available in df.schema.fields")

# COMMAND ----------

columns_to_check = ['status_name', 'status_type']

for column in columns_to_check:
    if any(field.name == column for field in df.schema.fields):
        print(f"Column '{column}' is available in df.Schema")
    else:
        print(f"Column '{column}' is not available in df.Schema")

# COMMAND ----------

columns_to_check = ['ObjectID', 'ID', 'Name', 'Date', 'contract_source', 'currency_code', 'refunded_amount', 'start_date', 'product_url', 'category', 'default_group', 'source_target', 'cloud_flatform', 'session_id', 'session_name', 'status_name', 'status_type', 'sessions', 'product_id', 'load datetime']

schema_field_names = [field.name for field in df.schema.fields]
non_available_columns = [col for col in columns_to_check if col not in schema_field_names]

print("Schema filed Names: \n", schema_field_names)
print("\nList all columns from columns_to_check that are NOT present in df.columns: \n", non_available_columns)
print("\nPrint Struct Field: \n", df.schema.fields)

if any(field.name == "sales_organisation_id" for field in df.schema.fields):
    print("\nColumn 'sales_organisation_id' is available in table_Schema.")
else:
    print("\nColumn 'sales_organisation_id' is not available in table_Schema.")

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Using a function to reuse the check**

# COMMAND ----------

def check_columns(df, cols):
    missing = [col for col in cols if col not in df.columns]
    if missing:
        print(f"Missing columns: {missing}")
        return False
    print("All columns are present")
    return True

# Example usage
check_columns(df, ["region", "sales_organisation_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Check and safely select only existing columns**
# MAGIC
# MAGIC - If you’re not sure **all columns exist** and want to **select only valid ones**.

# COMMAND ----------

safe_cols = [col for col in ["region", "sessions", "invalid_col", "status_type"] if col in df.columns]
df.select(*safe_cols).display()

# COMMAND ----------

# MAGIC %md
# MAGIC | Task                       | Best Method                                              | Example             |
# MAGIC | -------------------------- | -------------------------------------------------------- | ------------------- |
# MAGIC | Check one column           | `"col" in df.columns`                                    | ✅ Simple and fast   |
# MAGIC | Check multiple columns     | `all(col in df.columns for col in cols)`                 | ✅ Detect missing    |
# MAGIC | Check with schema info     | `any(field.name == "col" for field in df.schema.fields)` | ✅ Include type info |
# MAGIC | Automatically skip missing | `[c for c in cols if c in df.columns]`                   | ✅ Safe selection    |