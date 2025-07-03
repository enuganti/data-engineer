# Databricks notebook source
# MAGIC %md
# MAGIC **INTERSECT**
# MAGIC - The **INTERSECT** operator returns the **common rows** between the result sets of **two or more SELECT** statements.

# COMMAND ----------

# MAGIC %md
# MAGIC **Topics Covered**
# MAGIC - Ex 01: Basic INTERSECT usage
# MAGIC - Ex 02: Common Customers in Two Tables
# MAGIC - Ex 03: Different column orders, still works if data types and count match
# MAGIC - Ex 04: Error due to different column counts
# MAGIC - Ex 05: Case-sensitive match
# MAGIC - Ex 06: INTERSECT with Multiple Columns

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Basic Rules:
# MAGIC
# MAGIC **Same Number of Columns:**
# MAGIC - **Both SELECT queries** must return the **same number of columns**.
# MAGIC
# MAGIC **Same Data Types (or compatible types):**
# MAGIC - The corresponding columns must have **compatible data types**.
# MAGIC
# MAGIC **Column Order Matters:**
# MAGIC - The comparison is based on **column position, not name**.
# MAGIC
# MAGIC **Duplicates Removed:**
# MAGIC - INTERSECT returns only **distinct rows** that appear in **both result sets**.
# MAGIC - It **removes duplicates** by default. Duplicates are removed, the result is always **distinct rows**.
# MAGIC
# MAGIC **ORDER BY** must come **after** the INTERSECT block.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      SELECT column1, column2, ...
# MAGIC      FROM table1
# MAGIC      [WHERE condition]
# MAGIC      INTERSECT
# MAGIC      SELECT column1, column2, ...
# MAGIC      FROM table2;
# MAGIC      [WHERE condition]

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 01: Basic INTERSECT usage**
# MAGIC - Common **Country & City** in Two Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_intersect_Customers;
# MAGIC
# MAGIC CREATE TABLE tbl_intersect_Customers(
# MAGIC     ID INT,
# MAGIC     Name VARCHAR(20),
# MAGIC     Country VARCHAR(20),
# MAGIC     City VARCHAR(20)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tbl_intersect_Customers VALUES (1, 'Aakash', 'INDIA', 'Mumbai');
# MAGIC INSERT INTO tbl_intersect_Customers VALUES (2, 'George', 'USA', 'New York');
# MAGIC INSERT INTO tbl_intersect_Customers VALUES (3, 'David', 'INDIA', 'Bangalore');
# MAGIC INSERT INTO tbl_intersect_Customers VALUES (4, 'Leo', 'SPAIN', 'Madrid');
# MAGIC INSERT INTO tbl_intersect_Customers VALUES (5, 'Rahul', 'INDIA', 'Delhi');
# MAGIC INSERT INTO tbl_intersect_Customers VALUES (6, 'Brian', 'USA', 'Chicago');
# MAGIC INSERT INTO tbl_intersect_Customers VALUES (7, 'Justin', 'SPAIN', 'Barcelona');
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_Customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_intersect_Branches;
# MAGIC
# MAGIC CREATE TABLE tbl_intersect_Branches(
# MAGIC     Branch_Code INT,
# MAGIC     Country VARCHAR(20),
# MAGIC     City VARCHAR(20)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tbl_intersect_Branches VALUES (101, 'INDIA', 'Mumbai');
# MAGIC INSERT INTO tbl_intersect_Branches VALUES (201, 'INDIA', 'Bangalore');
# MAGIC INSERT INTO tbl_intersect_Branches VALUES (301, 'USA', 'Chicago');
# MAGIC INSERT INTO tbl_intersect_Branches VALUES (401, 'USA', 'New York');
# MAGIC INSERT INTO tbl_intersect_Branches VALUES (501, 'SPAIN', 'Madrid');
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_Branches;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Country, City FROM tbl_intersect_Customers
# MAGIC INTERSECT
# MAGIC SELECT Country, City FROM tbl_intersect_Branches
# MAGIC ORDER BY City;

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 02: Common Customers in Two Tables**
# MAGIC - **Result:** Employees who are part of **both departments**.

# COMMAND ----------

# DBTITLE 1,tbl_department_A
# MAGIC %sql
# MAGIC -- Step 1: Create department_A table
# MAGIC CREATE TABLE tbl_intersect_department_A (
# MAGIC     employee_id INT,
# MAGIC     employee_name VARCHAR(50)
# MAGIC );
# MAGIC
# MAGIC -- Step 2: Insert data into department_A
# MAGIC INSERT INTO tbl_intersect_department_A (employee_id, employee_name)
# MAGIC VALUES
# MAGIC (1, 'Alekya'),
# MAGIC (2, 'Baskar'),
# MAGIC (3, 'Chandra'),
# MAGIC (4, 'Darshan');
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_department_A;

# COMMAND ----------

# DBTITLE 1,tbl_department_B
# MAGIC %sql
# MAGIC
# MAGIC -- Step 1: Create department_B table
# MAGIC CREATE TABLE tbl_intersect_department_B (
# MAGIC     employee_id INT,
# MAGIC     employee_name VARCHAR(50)
# MAGIC );
# MAGIC
# MAGIC -- Step 2: Insert data into department_B
# MAGIC INSERT INTO tbl_intersect_department_B (employee_id, employee_name)
# MAGIC VALUES
# MAGIC (3, 'Chandra'),
# MAGIC (4, 'Darshan'),
# MAGIC (5, 'Sampath'),
# MAGIC (6, 'Nirosha');
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_department_B;

# COMMAND ----------

# DBTITLE 1,INTERSECT
# MAGIC %sql
# MAGIC -- Query to find common employees in both departments
# MAGIC SELECT employee_name FROM tbl_intersect_department_A
# MAGIC INTERSECT
# MAGIC SELECT employee_name FROM tbl_intersect_department_B;

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 03: Different column orders, still works if data types and count match**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tblStudents_2023 (
# MAGIC     student_id INT,
# MAGIC     student_name VARCHAR(50)
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE tblStudents_2024 (
# MAGIC     student_id INT,
# MAGIC     student_name VARCHAR(50)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tblStudents_2023 VALUES (1, 'John'), (2, 'Asha'), (3, 'David'), (4, 'Priya');
# MAGIC INSERT INTO tblStudents_2024 VALUES (2, 'Asha'), (3, 'David'), (5, 'Kiran');
# MAGIC
# MAGIC SELECT * FROM tblStudents_2023;
# MAGIC SELECT * FROM tblStudents_2024;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT student_id, student_name FROM tblStudents_2023
# MAGIC INTERSECT
# MAGIC SELECT student_id, student_name FROM tblStudents_2024;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Works because same data types and order is consistent even though column names are swapped.
# MAGIC SELECT student_name, student_id FROM tblStudents_2023
# MAGIC INTERSECT
# MAGIC SELECT student_name, student_id FROM tblStudents_2024;

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 04: Error due to different column counts**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Error: INTERSECT queries must have the same number of columns.
# MAGIC SELECT student_id FROM tblStudents_2023
# MAGIC INTERSECT
# MAGIC SELECT student_id, student_name FROM tblStudents_2024;

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 05: Case-sensitive match**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add this row to 2024
# MAGIC INSERT INTO tbl_intersect_Customers VALUES (8, 'Suresh', 'India', 'Mumbai');
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_Customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Country FROM tbl_intersect_Customers
# MAGIC INTERSECT
# MAGIC SELECT Country FROM tbl_intersect_Branches

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 06: INTERSECT with Multiple Columns**
# MAGIC - **Result:** Employees present in both years.

# COMMAND ----------

# DBTITLE 1,tbl_intersect_employees_2024
# MAGIC %sql
# MAGIC -- Step 1: Create employees_2024 table
# MAGIC DROP TABLE IF EXISTS tbl_intersect_employees_2024;
# MAGIC
# MAGIC CREATE TABLE tbl_intersect_employees_2024 (
# MAGIC     employee_id INT,
# MAGIC     first_name VARCHAR(50),
# MAGIC     last_name VARCHAR(50),
# MAGIC     department VARCHAR(50)
# MAGIC );
# MAGIC
# MAGIC -- Step 3: Insert data into employees_2024
# MAGIC INSERT INTO tbl_intersect_employees_2024 (employee_id, first_name, last_name, department)
# MAGIC VALUES
# MAGIC (1, 'Pandit', 'Smiti', 'HR'),
# MAGIC (2, 'Naresh', 'Kumar', 'Finance'),
# MAGIC (3, 'Mohan', 'Rao', 'IT'),
# MAGIC (4, 'Andy', 'Smith', 'Marketing');
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_employees_2024;

# COMMAND ----------

# DBTITLE 1,tbl_intersect_employees_2025
# MAGIC %sql
# MAGIC -- Step 2: Create employees_2025 table
# MAGIC DROP TABLE IF EXISTS tbl_intersect_employees_2025;
# MAGIC
# MAGIC CREATE TABLE tbl_intersect_employees_2025 (
# MAGIC     employee_id INT,
# MAGIC     first_name VARCHAR(50),
# MAGIC     last_name VARCHAR(50),
# MAGIC     department VARCHAR(50)
# MAGIC );
# MAGIC
# MAGIC -- Step 4: Insert data into employees_2025
# MAGIC INSERT INTO tbl_intersect_employees_2025 (employee_id, first_name, last_name, department)
# MAGIC VALUES
# MAGIC (5, 'Mohan', 'Rao', 'IT'),
# MAGIC (6, 'Andy', 'Smith', 'Sales'),
# MAGIC (7, 'Elango', 'Thiru', 'Finance'),
# MAGIC (8, 'Faisal', 'Ali', 'IT');
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_employees_2025;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INTERSECT Query to find common employees based on name
# MAGIC SELECT first_name, last_name FROM tbl_intersect_employees_2024
# MAGIC INTERSECT
# MAGIC SELECT first_name, last_name FROM tbl_intersect_employees_2025;

# COMMAND ----------

# MAGIC %md
# MAGIC **Summary**
# MAGIC
# MAGIC | Rule                     | Enforced? |
# MAGIC | ------------------------ | --------- |
# MAGIC | Same # of columns        | ✅ Yes     |
# MAGIC | Same data types          | ✅ Yes     |
# MAGIC | Duplicate rows removed   | ✅ Yes     |
# MAGIC | Case sensitive           | ✅ Yes     |
# MAGIC | ORDER BY only at the end | ✅ Yes     |