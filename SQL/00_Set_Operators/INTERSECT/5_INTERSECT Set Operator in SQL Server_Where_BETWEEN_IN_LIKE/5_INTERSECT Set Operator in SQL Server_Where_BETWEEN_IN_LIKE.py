# Databricks notebook source
# MAGIC %md
# MAGIC **INTERSECT**
# MAGIC - The **INTERSECT** operator returns the **common rows** between the result sets of **two or more SELECT** statements.

# COMMAND ----------

# MAGIC %md
# MAGIC **Topics Covered**
# MAGIC - Ex 01: Common IDs with Conditions (Using INTERSECT with WHERE Clause)
# MAGIC - Ex 02: INTERSECT with BETWEEN Operator
# MAGIC - Ex 03: INTERSECT with IN Operator
# MAGIC - Ex 04: INTERSECT with LIKE Operator
# MAGIC - Ex 05: INTERSECT Inside a View or Subquery

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 01: Common IDs with Conditions (Using INTERSECT with WHERE Clause)**
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
# MAGIC **Ex 02: INTERSECT with BETWEEN Operator**

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
# MAGIC **Ex 03: INTERSECT with IN Operator**

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
# MAGIC **Ex 04: INTERSECT with LIKE Operator** 

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
# MAGIC **Ex 05: INTERSECT Inside a View or Subquery**
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