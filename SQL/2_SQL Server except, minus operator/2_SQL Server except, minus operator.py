# Databricks notebook source
# MAGIC %md
# MAGIC **Except / Minus:**
# MAGIC
# MAGIC - The **Except / Minus** operator returns **all rows** from **table_A** that **do not exist** in **table_B**.
# MAGIC
# MAGIC - Returns rows from the result set of the **first SELECT** statement that are **not present** in the result set of the **second SELECT** statement.
# MAGIC
# MAGIC    - **Query1 - Query2**
# MAGIC    - It also **eliminates duplicate** rows.
# MAGIC
# MAGIC **What happens?**
# MAGIC
# MAGIC - SQL Server first collects all **distinct rows** from the **first SELECT**.
# MAGIC
# MAGIC - It then **removes** any rows that **exactly match a row in the second SELECT**.
# MAGIC
# MAGIC
# MAGIC **I have two tables and first table has 100 records and second table has 90 records. How to get those missed records?**

# COMMAND ----------

# MAGIC %md
# MAGIC **Basic Rules:**
# MAGIC
# MAGIC - The **number and the order** of the **columns** must be the **same** in **both the queries**.
# MAGIC - Corresponding columns must have **compatible data types**.
# MAGIC - It automatically **removes duplicates** (similar to DISTINCT).
# MAGIC   - If you want to **retain duplicates**, use **NOT EXISTS or LEFT JOIN ... IS NULL**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Valid in:**
# MAGIC - Oracle
# MAGIC - Snowflake
# MAGIC - Google BigQuery
# MAGIC
# MAGIC **Not valid in:**
# MAGIC - **SQL Server:**
# MAGIC   - Use **EXCEPT** instead.
# MAGIC - **MySQL**:
# MAGIC   - Doesn't support **MINUS or EXCEPT**.
# MAGIC   - Use **LEFT JOIN ... IS NULL**.
# MAGIC
# MAGIC - This is similar to **minus** operator in **oracle**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      SELECT <columns> FROM <TableA>
# MAGIC      [WHERE ...]
# MAGIC      EXCEPT
# MAGIC      SELECT <columns> FROM <TableB>
# MAGIC      [WHERE ...];

# COMMAND ----------

# DBTITLE 1,TableA
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Except_TableA;
# MAGIC
# MAGIC CREATE TABLE Except_TableA
# MAGIC (
# MAGIC  EmployeeID int,
# MAGIC  Name VARCHAR(50),
# MAGIC  Gender VARCHAR(10),
# MAGIC  Country VARCHAR(20),
# MAGIC  Designation VARCHAR(20)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO Except_TableA VALUES (101, 'Suhash', 'M', 'India', 'Manager');
# MAGIC INSERT INTO Except_TableA VALUES (102, 'Veena', 'F', 'India', 'Sr Manager');
# MAGIC INSERT INTO Except_TableA VALUES (103, 'Sathya', 'M', 'India', 'Associate');
# MAGIC INSERT INTO Except_TableA VALUES (104, 'Marc', 'M', 'US', 'Project Manager');
# MAGIC INSERT INTO Except_TableA VALUES (105, 'Brad', 'M', 'Sweden', 'Dy Manager');
# MAGIC INSERT INTO Except_TableA VALUES (106, 'Paul', 'M', 'AUS', 'Lead');
# MAGIC INSERT INTO Except_TableA VALUES (107, 'Srinidi', 'F', 'India', 'DGM');
# MAGIC
# MAGIC SELECT * FROM Except_TableA;

# COMMAND ----------

# DBTITLE 1,TableB
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Except_TableB;
# MAGIC
# MAGIC CREATE TABLE Except_TableB
# MAGIC (
# MAGIC  EmployeeID int,
# MAGIC  Name VARCHAR(50),
# MAGIC  Gender VARCHAR(10),
# MAGIC  Country VARCHAR(20),
# MAGIC  Designation VARCHAR(20)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO Except_TableB VALUES (101, 'Swapna', 'F', 'India', 'DGM');
# MAGIC INSERT INTO Except_TableB VALUES (102, 'Tanvee', 'F', 'India', 'Manager');
# MAGIC INSERT INTO Except_TableB VALUES (103, 'Somesh', 'M', 'India', 'Jr Manager');
# MAGIC INSERT INTO Except_TableB VALUES (104, 'Marc', 'M', 'US', 'Project Manager'); -- common records b/n TableA and TableB
# MAGIC INSERT INTO Except_TableB VALUES (105, 'Brad', 'M', 'Sweden', 'Dy Manager');  -- common records b/n TableA and TableB
# MAGIC INSERT INTO Except_TableB VALUES (106, 'Lopa', 'F', 'UK', 'Lead');
# MAGIC INSERT INTO Except_TableB VALUES (107, 'Srinivas', 'M', 'India', 'AGM');
# MAGIC
# MAGIC SELECT * FROM Except_TableB;

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Comparing Subsets of Columns**

# COMMAND ----------

# DBTITLE 1,Primary Key
# MAGIC %sql
# MAGIC SELECT EmployeeID
# MAGIC FROM Except_TableA
# MAGIC
# MAGIC EXCEPT
# MAGIC
# MAGIC SELECT EmployeeID
# MAGIC FROM Except_TableB;

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Returns the unique rows from the left query that aren’t in the right query’s results**
# MAGIC - The **number and the order** of the **columns** are **same** in both the queries.
# MAGIC - The **data types** are **same**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT EmployeeID, Name, Gender, Country, Designation
# MAGIC FROM Except_TableA
# MAGIC EXCEPT
# MAGIC SELECT EmployeeID, Name, Gender, Country, Designation
# MAGIC FROM Except_TableB

# COMMAND ----------

# MAGIC %md
# MAGIC **Order By:**
# MAGIC - If you want the **final result ordered**, you can put **ORDER BY** only after the **second SELECT**.

# COMMAND ----------

# DBTITLE 1,ORDER BY
# MAGIC %sql
# MAGIC SELECT EmployeeID, Name, Gender, Country, Designation
# MAGIC FROM Except_TableA
# MAGIC EXCEPT
# MAGIC SELECT EmployeeID, Name, Gender, Country, Designation
# MAGIC FROM Except_TableB
# MAGIC ORDER BY EmployeeID;

# COMMAND ----------

# MAGIC %md
# MAGIC **3) To retrieve all of the rows from Table B that does not exist in Table A, reverse the two queries as shown below**
# MAGIC - The **number and the order** of the **columns** are **same** in both the queries.
# MAGIC - The **data types** are **same**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT EmployeeID, Name, Gender, Country, Designation
# MAGIC FROM Except_TableB
# MAGIC EXCEPT
# MAGIC SELECT EmployeeID, Name, Gender, Country, Designation
# MAGIC FROM Except_TableA

# COMMAND ----------

# MAGIC %md
# MAGIC **4) [NUM_COLUMNS_MISMATCH]**
# MAGIC - **Unequal number** of columns
# MAGIC - The **data types** are **same**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT EmployeeID, Name, Gender, Country, Designation
# MAGIC FROM Except_TableA
# MAGIC EXCEPT
# MAGIC SELECT EmployeeID, Name, Gender, Country
# MAGIC FROM Except_TableB

# COMMAND ----------

# MAGIC %md
# MAGIC **4) [NUM_COLUMNS_MISMATCH]**
# MAGIC - **Number of Columns** are **Same**.
# MAGIC - Columns **order is different**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Name, EmployeeID, Gender, Country, Designation
# MAGIC FROM Except_TableA
# MAGIC EXCEPT
# MAGIC SELECT EmployeeID, Name, Gender, Country, Designation
# MAGIC FROM Except_TableB

# COMMAND ----------

# MAGIC %md
# MAGIC | TableA Field | TableB Field | Result     |
# MAGIC | ------------ | ------------ | ---------- |
# MAGIC | Name         | Id           | mismatched |
# MAGIC | Id           | Name         | mismatched |
# MAGIC | Gender       | Gender       | matched    |
# MAGIC | Country      | Country      | matched    |
# MAGIC | Designation  | Designation  | matched    |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - Because most rows have **different Name vs Id values**, **none matched.** So EXCEPT returned **all rows** from **TableA**.

# COMMAND ----------

# MAGIC %md
# MAGIC **5) The column names do not have to be the same:**
# MAGIC
# MAGIC - Even though **id ≠ emp_id** and **name ≠ emp_name**, this will still work as long as:
# MAGIC
# MAGIC   - Both queries return the **same number of columns**.
# MAGIC   - The **data types** of corresponding columns match or are compatible.
# MAGIC
# MAGIC **Summary**:
# MAGIC
# MAGIC ❌ **Column names:** Don't need to match.
# MAGIC
# MAGIC ✅ **Number of columns:** Must match.
# MAGIC
# MAGIC ✅ **Data types:** Must be compatible.

# COMMAND ----------

# DBTITLE 1,TableC
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Except_TableC;
# MAGIC
# MAGIC Create Table Except_TableC
# MAGIC (
# MAGIC  emp_id int,
# MAGIC  emp_name VARCHAR(50),
# MAGIC  Gender VARCHAR(10),
# MAGIC  Country VARCHAR(20),
# MAGIC  Designation VARCHAR(20)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO Except_TableC VALUES (101, 'Swapna', 'F', 'India', 'DGM');
# MAGIC INSERT INTO Except_TableC VALUES (102, 'Tanvee', 'F', 'India', 'Manager');
# MAGIC INSERT INTO Except_TableC VALUES (103, 'Somesh', 'M', 'India', 'Jr Manager');
# MAGIC INSERT INTO Except_TableC VALUES (104, 'Marc', 'M', 'US', 'Project Manager'); -- common records b/n TableA and TableB
# MAGIC INSERT INTO Except_TableC VALUES (105, 'Brad', 'M', 'Sweden', 'Dy Manager');  -- common records b/n TableA and TableB
# MAGIC INSERT INTO Except_TableC VALUES (106, 'Lopa', 'F', 'UK', 'Lead');
# MAGIC INSERT INTO Except_TableC VALUES (107, 'Srinivas', 'M', 'India', 'AGM');
# MAGIC
# MAGIC SELECT * FROM Except_TableC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT EmployeeID, Name, Gender, Country, Designation
# MAGIC FROM Except_TableA
# MAGIC EXCEPT
# MAGIC SELECT emp_id, emp_name, Gender, Country, Designation
# MAGIC FROM Except_TableC;