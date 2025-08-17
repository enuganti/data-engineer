# Databricks notebook source
# MAGIC %md
# MAGIC #### ISNULL() / IFNULL()
# MAGIC
# MAGIC - The ISNULL() function in SQL Server **replaces NULL values** with a **specified replacement value**.
# MAGIC - used to **replace NULL values** with a **specified replacement value**.
# MAGIC - The ISNULL() function is used to **return a specified value** if the expression is **NULL**, otherwise it returns the **expression** itself.

# COMMAND ----------

# MAGIC %md
# MAGIC - **IFNULL()** is a **MySQL** function (similar to **ISNULL() in SQL Server**).
# MAGIC
# MAGIC **SQL Server:**
# MAGIC - isnull
# MAGIC - coalesce
# MAGIC
# MAGIC **MySQL:**
# MAGIC - iFnull
# MAGIC - coalesce
# MAGIC
# MAGIC **Oracle:**
# MAGIC - nvl
# MAGIC - coalesce

# COMMAND ----------

# MAGIC %md
# MAGIC      SELECT IFNULL(Status, 'Finished') AS Status
# MAGIC      FROM tbl_NonNull_Nulls_Blank; 
# MAGIC                   (or)
# MAGIC      SELECT COALESCE(Status, 'Finished') AS Status
# MAGIC      FROM tbl_NonNull_Nulls_Blank;
# MAGIC
# MAGIC ❌ Avoid in Spark SQL:
# MAGIC
# MAGIC      ISNULL(Status, 'Finished')  -- ❌ This is for SQL Server, not Spark

# COMMAND ----------

# MAGIC %md
# MAGIC #### Syntax
# MAGIC
# MAGIC      ISNULL(expression, replacement_value)
# MAGIC
# MAGIC - **expression:** The value/column to check for **NULL**.
# MAGIC - **replacement_value:** Value to return if **expression is NULL**.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT IFNULL(NULL, 'Database Engineer');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT IFNULL('Hello', 'Database Engineer') AS Name;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tblEmployeesIsNull;
# MAGIC CREATE TABLE tblEmployeesIsNull (
# MAGIC     OBJECT_ID INT,
# MAGIC     Name VARCHAR(50),
# MAGIC     Department VARCHAR(30),
# MAGIC     Salary DECIMAL(10,2),
# MAGIC     Bonus DECIMAL(10,2),
# MAGIC     Remarks VARCHAR(100),
# MAGIC     ContactPhone VARCHAR(15),
# MAGIC     Status VARCHAR(15),
# MAGIC     description VARCHAR(20),
# MAGIC     weight int,
# MAGIC     Feedback VARCHAR(20)
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO tblEmployeesIsNull VALUES
# MAGIC (583069, 'Harish', 'Admin', 50000, NULL, 'Good Attendance', NULL, NULL, 'E-Mail', 100, 'Regular'),
# MAGIC (510102, "", 'HR', 45000, 5000, 'Regular Attendance and studious', NULL, 'Finished', 'Internet', 200, NULL),
# MAGIC (506654, "Basha", "", 60000, 0, '', '9876543210', 'Not Relevant', 'Social Media', NULL, ''),
# MAGIC (583195, NULL, 'IT', 52000, 4000, NULL, NULL, 'Started', 'Messaging', 10, 'Good'),
# MAGIC (470450, "Venky", 'SALES', 50000, NULL, 'Good Attendance', NULL, 'Not Relevant', 'IoT', 50, ''),
# MAGIC (558253, "", 'HR', 45000, 5000, 'Regular Attendance and studious', NULL, 'Open', NULL, 75, NULL),
# MAGIC (NULL, "Krishna", NULL, 60000, NULL, 'After markets', '9876543210', NULL, 'Manual data entry', NULL, 'Regular'),
# MAGIC (583181, "Kiran", 'IT', 52000, 4000, NULL, NULL, 'Finished', 'Other', NULL, 'Medium'),
# MAGIC (583119, "Hitesh", 'Finance', 60000, NULL, '', '9876543210', NULL, 'Telephony', 300, 'Average'),
# MAGIC (577519, "", NULL, 52000, 4000, NULL, NULL, 'Not Relevant', NULL, 500, 'Good'),
# MAGIC (583151, "Sushma", 'Accounts', 52000, 4000, 'Explained in detail', NULL, 'Open', 'Fax', NULL, ''),
# MAGIC (583167, NULL, NULL, 52000, 0, '', NULL, 'Not Relevant', 'Feedback', 250, 'Regular'),
# MAGIC (583162, "Buvan", NULL, 52000, 4000, NULL, NULL, NULL, 'WorkZone', 350, NULL),
# MAGIC (575216, "Mohan", 'Hardware', 52000, 4000, 'Nice Explanation', NULL, 'Open', 'IOT', NULL, ''),
# MAGIC (NULL, NULL, NULL, 52000, 0, '', NULL, 'Finished', NULL, 1000, 'Average'),
# MAGIC (583173, "Lohith", "", 52000, 4000, NULL, NULL, 'Finished', NULL, 550, 'Good'),
# MAGIC (583099, "Loba", 'Maintenance', 52000, 0, '', NULL, 'Started', NULL, 650, 'Excellent');
# MAGIC
# MAGIC SELECT * FROM tblEmployeesIsNull;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Replace NULL Values with Default
# MAGIC - **Replaces NULL** in Bonus column with **0**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT OBJECT_ID, Name, Department,
# MAGIC        IFNULL(Bonus, 0) AS Bonus
# MAGIC FROM tblEmployeesIsNull;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Replace NULL with Text
# MAGIC - Shows **Not Assigned** if Department is **NULL**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT OBJECT_ID, Name,
# MAGIC        IFNULL(Department, 'Not Assigned') AS Department
# MAGIC FROM tblEmployeesIsNull;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) How to take sum / average?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT weight,
# MAGIC        IFNULL(weight, 100) Replace_Null
# MAGIC FROM tblEmployeesIsNull;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query using ISNULL to replace NULL weights with 100, then take average
# MAGIC SELECT AVG(IFNULL(weight, 100)) AS AvgWeight,
# MAGIC        SUM(IFNULL(weight, 100)) AS SumWeight
# MAGIC FROM tblEmployeesIsNull;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Use ISNULL() in a Calculation
# MAGIC - Adds **Salary with Bonus**, treating missing bonus as **0**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT OBJECT_ID, Name, Salary, Bonus,
# MAGIC        Salary + IFNULL(Bonus, 750) AS TotalPay
# MAGIC FROM tblEmployeesIsNull;