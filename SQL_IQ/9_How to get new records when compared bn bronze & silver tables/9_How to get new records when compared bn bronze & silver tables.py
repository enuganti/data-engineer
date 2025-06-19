# Databricks notebook source
# MAGIC %md
# MAGIC - To get **new records** from **one table** when compared to **another** based on a **primary key**, you typically use below approach.
# MAGIC
# MAGIC   - NOT EXISTS
# MAGIC   - LEFT JOIN ... IS NULL
# MAGIC   - NOT IN
# MAGIC
# MAGIC Let's assume:
# MAGIC
# MAGIC - **DBA_Data** is the **source** (e.g., latest or updated data).
# MAGIC - **DBA_Details** is the **target** (e.g., existing or old data).
# MAGIC - **EMP_ID** is the **primary key** in **both** tables.

# COMMAND ----------

# MAGIC %md
# MAGIC - **How to Select All Records from One Table That Do Not Exist in Another Table in SQL?**
# MAGIC
# MAGIC - **Write a SQL query to find records in Table A that are not in Table B without using NOT IN operator.**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS DBA_Data;
# MAGIC
# MAGIC CREATE TABLE DBA_Data (
# MAGIC  EMP_ID VARCHAR(20),
# MAGIC  EMP_Name VARCHAR(15),
# MAGIC  EMP_Salary INT,
# MAGIC  EMP_Age INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DBA_Data
# MAGIC VALUES
# MAGIC ("SA001", "Naresh", 20000, 26),
# MAGIC ("SA002", "Kumar", 25000, 27),
# MAGIC ("SA003", "Rahul", 29000, 29),
# MAGIC ("SA004", "Krishna", 32000, 31),
# MAGIC ("SA005", "David", 35000, 33),
# MAGIC ("SA006", "Himaja", 39000, 35);
# MAGIC
# MAGIC SELECT * FROM DBA_Data;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS DBA_Details;
# MAGIC
# MAGIC CREATE TABLE DBA_Details (
# MAGIC  EMP_ID VARCHAR(20),
# MAGIC  EMP_Name VARCHAR(15),
# MAGIC  EMP_Designation VARCHAR(15),
# MAGIC  EMP_Age INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DBA_Details
# MAGIC VALUES
# MAGIC ("SA001", "Naresh", "SW Developer", 32),
# MAGIC ("SA003", "Rohit", "Web Developer", 34);
# MAGIC
# MAGIC SELECT * FROM DBA_Details;

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Using NOT EXISTS**
# MAGIC - This gives you all rows in **DBA_Data** where the **EMP_ID** does **not exist** in **DBA_Details** — i.e., **new records**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM DBA_Data a
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT EMP_ID
# MAGIC     FROM DBA_Details b
# MAGIC     WHERE a.EMP_ID = b.EMP_ID
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Using LEFT JOIN ... IS NULL**
# MAGIC - This joins both tables and filters out rows where there's **no match** in **DBA_Details**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT a.*, b.*
# MAGIC SELECT *
# MAGIC FROM DBA_Data a
# MAGIC LEFT JOIN DBA_Details b ON a.EMP_ID = b.EMP_ID;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.*
# MAGIC FROM DBA_Data a
# MAGIC LEFT JOIN DBA_Details b ON a.EMP_ID = b.EMP_ID
# MAGIC WHERE b.EMP_ID IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Using NOT IN (Be careful with NULLs)**
# MAGIC
# MAGIC **Note:**
# MAGIC - This can behave unexpectedly if **table_b.id** has any **NULL** values — it will return **zero rows**.
# MAGIC - Prefer **NOT EXISTS or LEFT JOIN** unless you're sure **id is NOT NULL**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DBA_Data
# MAGIC WHERE EMP_ID NOT IN (
# MAGIC     SELECT EMP_ID
# MAGIC     FROM DBA_Details
# MAGIC );