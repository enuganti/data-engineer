# Databricks notebook source
# MAGIC %md
# MAGIC #### 1) ISNULL() / IFNULL()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tblProductIsNull;
# MAGIC CREATE TABLE tblProductIsNull(
# MAGIC      SNo int,
# MAGIC      first_name varchar(20),
# MAGIC      middle_name varchar(20),
# MAGIC      last_name varchar(20),
# MAGIC      Product varchar(20),
# MAGIC      ProductsSold int,
# MAGIC      ItemsOrder int,
# MAGIC      ItemPrice int
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tblProductIsNull(SNo, first_name, middle_name, last_name, Product, ProductsSold, ItemsOrder, ItemPrice)
# MAGIC VALUES
# MAGIC (101, 'Rupesh', null, null, 'nut', 100, 50, 200),
# MAGIC (102, 'Roshan', null, 'Swetha', 'bag', 200, null, 300),
# MAGIC (103, 'Rashi', 'Govardan', null, 'plug', 150, 50, 100),
# MAGIC (104, null, 'Kasi', 'Gupta', 'cover', 125, null, 400),
# MAGIC (105, 'Ganesh', 'Kumar', 'Rao', 'light', 100, 25, 500),
# MAGIC (106, 'Danush', 'Kannan', null, 'plywood', 120, 50, 150),
# MAGIC (107, null, 'Kalki', 'Rayudu', 'glass', 120, null, 200),
# MAGIC (108, 'Gopesh', 'Singh', null, 'light', 200, 35, 300),
# MAGIC (109, null, null, 'Dorai', 'vinyl', 50, 35, 100),
# MAGIC (110, null, null, null, 'screw', 100, 35, 200);
# MAGIC
# MAGIC SELECT * FROM tblProductIsNull;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name,
# MAGIC        IFNULL(first_name, 'FIRST-NAME') AS FullName,
# MAGIC        middle_name,
# MAGIC        IFNULL(middle_name, 'MIDDLE-NAME') AS MiddleName,
# MAGIC        last_name,
# MAGIC        IFNULL(last_name, 'LAST-NAME') AS LastName
# MAGIC FROM tblProductIsNull;

# COMMAND ----------

# DBTITLE 1,diff b/n isnull() & coalesce
# MAGIC %sql
# MAGIC SELECT first_name, middle_name, last_name,
# MAGIC        IFNULL(first_name, middle_name) AS FullName,
# MAGIC        COALESCE(first_name, middle_name, last_name, 'Default') AS CustName
# MAGIC FROM tblProductIsNull;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name, middle_name, last_name, 
# MAGIC        CONCAT(first_name, ' ', middle_name, ' ', last_name) AS FullName,
# MAGIC        ProductsSold, ItemsOrder, ItemPrice,
# MAGIC        (ProductsSold + ItemsOrder) * ItemPrice AS Total_Sales
# MAGIC FROM tblProductIsNull;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name, middle_name, last_name,
# MAGIC        CONCAT(IFNULL(first_name, 'FIRST-NAME'), ' ', IFNULL(middle_name, 'MIDDLE-NAME'), ' ', IFNULL(last_name, 'LAST-NAME')) AS FullName,
# MAGIC        ProductsSold, ItemsOrder, ItemPrice, 
# MAGIC        (ProductsSold + IFNULL(ItemsOrder, 0)) * ItemPrice AS Total_Sales
# MAGIC FROM tblProductIsNull;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) NOT ISNULL()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tblEmployeesNotIsNull;
# MAGIC CREATE TABLE tblEmployeesNotIsNull (
# MAGIC     EmpID INT,
# MAGIC     Name VARCHAR(50),
# MAGIC     Bonus DECIMAL(10,2)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tblEmployeesNotIsNull (EmpID, Name, Bonus) VALUES
# MAGIC (1, 'Joseph', 5000.00),
# MAGIC (2, 'Anand', NULL),
# MAGIC (3, 'Anand', 0),
# MAGIC (4, 'Robert', 3000.00),
# MAGIC (5, 'Megha', NULL),
# MAGIC (6, 'Dravid', 4500.00),
# MAGIC (7, 'Anand', 0);
# MAGIC
# MAGIC SELECT * FROM tblEmployeesNotIsNull;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### a) Using NOT ISNULL() in a WHERE clause
# MAGIC - **ISNULL(Bonus, 0)** replaces **NULL with 0**.
# MAGIC - **NOT ISNULL(Bonus, 0) = 0** means we are **excluding** rows where **Bonus is NULL or 0**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM tblEmployeesNotIsNull
# MAGIC WHERE NOT IFNULL(Bonus, 0) = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM tblEmployeesNotIsNull
# MAGIC WHERE IFNULL(Bonus, 0) = 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Count rows with non-null and non-zero Bonus

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CountValidBonus
# MAGIC FROM tblEmployeesNotIsNull
# MAGIC WHERE NOT IFNULL(Bonus, 0) = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CountValidBonus
# MAGIC FROM tblEmployeesNotIsNull
# MAGIC WHERE IFNULL(Bonus, 0) = 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### c) Filtering rows that have an actual value (non-zero)
# MAGIC - Returns only **employees** whose **bonus is greater than or equal to 1** after replacing **NULLs with 0**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM tblEmployeesNotIsNull
# MAGIC WHERE NOT IFNULL(Bonus, 0) < 1;