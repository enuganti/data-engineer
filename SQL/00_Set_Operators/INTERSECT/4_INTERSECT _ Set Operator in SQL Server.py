# Databricks notebook source
# MAGIC %md
# MAGIC **INTERSECT**
# MAGIC - The **INTERSECT** operator returns the **common rows** between the result sets of **two or more SELECT** statements.

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
# MAGIC - Common Country & City in Two Tables

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
# MAGIC - **Result:** Employees who are part of both departments.

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
# MAGIC **Ex 04: Case-sensitive match**

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
# MAGIC **Ex 05: INTERSECT with Multiple Columns**
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
# MAGIC **Ex 06: Common IDs with Conditions (Using INTERSECT with WHERE Clause)**
# MAGIC - **Result:** Orders that were delivered and then accepted as returns.

# COMMAND ----------

# DBTITLE 1,tbl_orders
# MAGIC %sql
# MAGIC -- Step 1: Create the `orders` table
# MAGIC DROP TABLE IF EXISTS tbl_intersect_orders;
# MAGIC
# MAGIC CREATE TABLE tbl_intersect_orders (
# MAGIC     id INT,
# MAGIC     customer_name VARCHAR(50),
# MAGIC     order_status VARCHAR(20)
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Step 2: Insert sample data into `orders`
# MAGIC INSERT INTO tbl_intersect_orders (id, customer_name, order_status)
# MAGIC VALUES
# MAGIC (101, 'Bibin', 'Delivered'),
# MAGIC (102, 'Charan', 'Cancelled'),
# MAGIC (103, 'Senthil', 'Delivered'),
# MAGIC (104, 'Prakash', 'Processing'),
# MAGIC (105, 'Vivek', 'Delivered');
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_orders; 

# COMMAND ----------

# DBTITLE 1,tbl_returns
# MAGIC %sql
# MAGIC -- Step 1: Create the `returns` table
# MAGIC DROP TABLE IF EXISTS tbl_intersect_returns;
# MAGIC
# MAGIC CREATE TABLE tbl_intersect_returns (
# MAGIC     id INT,
# MAGIC     return_status VARCHAR(20),
# MAGIC     return_reason VARCHAR(100)
# MAGIC );
# MAGIC
# MAGIC -- Step 2: Insert sample data into `returns`
# MAGIC INSERT INTO tbl_intersect_returns (id, return_status, return_reason)
# MAGIC VALUES
# MAGIC (101, 'Accepted', 'Damaged product'),
# MAGIC (103, 'Pending', 'Late delivery'),
# MAGIC (105, 'Accepted', 'Wrong item'),
# MAGIC (106, 'Accepted', 'Duplicate order');
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_returns;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id FROM tbl_intersect_orders
# MAGIC WHERE order_status = 'Delivered';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id FROM tbl_intersect_returns
# MAGIC WHERE return_status = 'Accepted';

# COMMAND ----------

# DBTITLE 1,INTERSECT
# MAGIC %sql
# MAGIC -- INTERSECT Query to get common IDs
# MAGIC SELECT id FROM tbl_intersect_orders
# MAGIC WHERE order_status = 'Delivered'
# MAGIC INTERSECT
# MAGIC SELECT id FROM tbl_intersect_returns
# MAGIC WHERE return_status = 'Accepted';

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 07: INTERSECT with BETWEEN Operator**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_intersect_students;
# MAGIC
# MAGIC CREATE TABLE tbl_intersect_students(
# MAGIC    ID INT, 
# MAGIC    NAME VARCHAR(20), 
# MAGIC    SUBJECT VARCHAR(20), 
# MAGIC    AGE INT, 
# MAGIC    HOBBY VARCHAR(20)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tbl_intersect_students
# MAGIC VALUES
# MAGIC (1, 'Naina', 'Maths', 24, 'Cricket'),
# MAGIC (2, 'Varun', 'Physics', 26, 'Football'),
# MAGIC (3, 'Dev', 'Maths', 23, 'Cricket'),
# MAGIC (4, 'Priya', 'Physics', 25, 'Cricket'),
# MAGIC (5, 'Aditya', 'Chemistry', 21, 'Cricket'),
# MAGIC (6, 'Kalyan', 'Maths', 30, 'Football');
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_students;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_intersect_students_hobby;
# MAGIC
# MAGIC CREATE TABLE tbl_intersect_students_hobby(
# MAGIC    ID INT, 
# MAGIC    NAME VARCHAR(20), 
# MAGIC    HOBBY VARCHAR(20), 
# MAGIC    AGE INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tbl_intersect_students_hobby
# MAGIC VALUES
# MAGIC (1, 'Vijay', 'Cricket', 18),
# MAGIC (2, 'Varun', 'Football', 26),
# MAGIC (3, 'Surya', 'Cricket', 19),
# MAGIC (4, 'Karthik', 'Cricket', 25),
# MAGIC (5, 'Sunny', 'Football', 26),
# MAGIC (6, 'Dev', 'Cricket', 23);
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_students_hobby;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NAME, AGE, HOBBY FROM tbl_intersect_students_hobby
# MAGIC WHERE AGE BETWEEN 25 AND 30;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NAME, AGE, HOBBY FROM tbl_intersect_students
# MAGIC WHERE AGE BETWEEN 20 AND 30;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NAME, AGE, HOBBY FROM tbl_intersect_students_hobby
# MAGIC WHERE AGE BETWEEN 25 AND 30
# MAGIC INTERSECT
# MAGIC SELECT NAME, AGE, HOBBY FROM tbl_intersect_students
# MAGIC WHERE AGE BETWEEN 20 AND 30;

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 08: INTERSECT with IN Operator**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NAME, AGE, HOBBY FROM tbl_intersect_students_hobby
# MAGIC WHERE HOBBY IN('Cricket');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NAME, AGE, HOBBY FROM tbl_intersect_students
# MAGIC WHERE HOBBY IN('Cricket');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NAME, AGE, HOBBY FROM tbl_intersect_students_hobby
# MAGIC WHERE HOBBY IN('Cricket')
# MAGIC INTERSECT
# MAGIC SELECT NAME, AGE, HOBBY FROM tbl_intersect_students
# MAGIC WHERE HOBBY IN('Cricket');

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 09: INTERSECT with LIKE Operator** 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NAME, AGE, HOBBY FROM tbl_intersect_students_hobby
# MAGIC WHERE NAME LIKE 'V%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NAME, AGE, HOBBY FROM tbl_intersect_students
# MAGIC WHERE NAME LIKE 'V%';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NAME, AGE, HOBBY FROM tbl_intersect_students_hobby
# MAGIC WHERE NAME LIKE 'V%'
# MAGIC INTERSECT
# MAGIC SELECT NAME, AGE, HOBBY FROM tbl_intersect_students
# MAGIC WHERE NAME LIKE 'V%';

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 10: INTERSECT Inside a View or Subquery**
# MAGIC - **Result:** Products available in both warehouse and store.

# COMMAND ----------

# DBTITLE 1,tbl_intersect_warehouse_stock
# MAGIC %sql
# MAGIC -- Step 1: Create warehouse_stock table
# MAGIC CREATE TABLE tbl_intersect_warehouse_stock (
# MAGIC     product_id INT,
# MAGIC     product_name VARCHAR(100),
# MAGIC     quantity INT
# MAGIC );
# MAGIC
# MAGIC -- Step 2: Insert data into warehouse_stock
# MAGIC INSERT INTO tbl_intersect_warehouse_stock (product_id, product_name, quantity)
# MAGIC VALUES
# MAGIC (101, 'Laptop', 50),
# MAGIC (102, 'Mouse', 200),
# MAGIC (103, 'Keyboard', 150),
# MAGIC (104, 'Monitor', 75);
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_warehouse_stock;

# COMMAND ----------

# DBTITLE 1,tbl_intersect_store_stock
# MAGIC %sql
# MAGIC -- Step 2: Create store_stock table
# MAGIC CREATE TABLE tbl_intersect_store_stock (
# MAGIC     product_id INT,
# MAGIC     product_name VARCHAR(100),
# MAGIC     quantity INT
# MAGIC );
# MAGIC
# MAGIC -- Step 4: Insert data into store_stock
# MAGIC INSERT INTO tbl_intersect_store_stock (product_id, product_name, quantity)
# MAGIC VALUES
# MAGIC (102, 'Mouse', 30),
# MAGIC (103, 'Keyboard', 25),
# MAGIC (105, 'Printer', 10),
# MAGIC (106, 'Webcam', 40);
# MAGIC
# MAGIC SELECT * FROM tbl_intersect_store_stock;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC     SELECT product_id FROM tbl_intersect_warehouse_stock
# MAGIC     INTERSECT
# MAGIC     SELECT product_id FROM tbl_intersect_store_stock
# MAGIC ) AS available_everywhere;

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