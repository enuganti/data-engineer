# Databricks notebook source
# MAGIC %md
# MAGIC #### How to Check whether Column is Empty or NULL?
# MAGIC
# MAGIC - There can be times when there is **NULL data** or **Column Value is Empty ('')**.
# MAGIC - There are many methods or functions to **identify the NULL or Empty values** and Below are some of them:
# MAGIC   - IS NULL
# MAGIC   - IS NOT NULL
# MAGIC   - ISNULL() Function
# MAGIC   - NOT ISNULL() Function
# MAGIC   - LEN()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample table
# MAGIC DROP TABLE IF EXISTS StudentsInfo;
# MAGIC CREATE TABLE StudentsInfo (
# MAGIC     StudID INT,
# MAGIC     StudentName VARCHAR(50),
# MAGIC     StudentClass VARCHAR(10),
# MAGIC     Remarks VARCHAR(100),
# MAGIC     ContactPhone VARCHAR(15)
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO StudentsInfo VALUES
# MAGIC (1, 'Sunder', 'XII A', 'Good', NULL),
# MAGIC (2, 'Rajendra Kumar', 'XII A', 'Regular', '9987654321'),
# MAGIC (3, 'Ratnakar', 'XII A', NULL, NULL),
# MAGIC (4, 'Pradeep', 'XII B', '', NULL),
# MAGIC (5, 'Rajamohan K', 'XII B', ' ', '9876543210'),
# MAGIC (6, 'Swaroop', 'XI A', NULL, NULL),
# MAGIC (7, 'Joseph', 'XI A', 'Regular', '5544339977'),
# MAGIC (8, 'Rajasekar', 'XI B', '', NULL),
# MAGIC (9, 'Hitesh', 'XII B', 'Medium', '8865123456'),
# MAGIC (10, 'Arun', 'XII B', NULL, NULL),
# MAGIC (11, 'Swetha', 'XI A', 'Average', '7890054321'),
# MAGIC (12, 'Jyoti', 'XI A', ' ', NULL),
# MAGIC (13, 'Swaroop', 'XI B', NULL, NULL),
# MAGIC (14, 'Hemanth', 'XII B', 'Medium', '7766554422'),
# MAGIC (15, 'Rakesh', 'XII B', '', NULL);
# MAGIC
# MAGIC SELECT * FROM StudentsInfo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to check NULL values in Remarks
# MAGIC SELECT *
# MAGIC FROM StudentsInfo
# MAGIC WHERE Remarks IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to check NULL values in Remarks
# MAGIC SELECT Remarks
# MAGIC FROM StudentsInfo
# MAGIC WHERE Remarks IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to check NULL values in ContactPhone
# MAGIC SELECT *
# MAGIC FROM StudentsInfo
# MAGIC WHERE ContactPhone IS NULL;
# MAGIC
# MAGIC -- SELECT ContactPhone
# MAGIC -- FROM StudentsInfo
# MAGIC -- WHERE ContactPhone IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to check NULL values in Remarks
# MAGIC SELECT * FROM StudentsInfo
# MAGIC WHERE ContactPhone IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC - It is to be noted that **empty string value or column** with **spaces** are displayed in the output if it is there and only NULL is filtered out.
# MAGIC - If we check the below query you can understand this as this column has **EMPTY ('') and SPACES (' ')**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to check NULL values in Remarks
# MAGIC SELECT * FROM StudentsInfo
# MAGIC WHERE Remarks IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) ISNULL()
# MAGIC - Returns all records with **NULL** values and records having **Empty value ('')** or **Spaces (' ')** in the specified **column**.

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
# MAGIC ##### a) Filter Rows Where Bonus is NULL
# MAGIC - Finds employees with **NULL** bonus values **(treated as 0)**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM tblEmployeesIsNull
# MAGIC WHERE IFNULL(Bonus, 0) = 0;

# COMMAND ----------

# MAGIC %md
# MAGIC **IFNULL(Bonus, 0)**
# MAGIC
# MAGIC - It **checks** if the **Bonus** value is **NULL**.
# MAGIC - If **Bonus** is **NULL**, it returns **0** instead.
# MAGIC - If **Bonus** is **NOT NULL**, it returns the **actual Bonus value**.
# MAGIC
# MAGIC **= 0**
# MAGIC
# MAGIC - This **filters** the **rows** so that:
# MAGIC   - If Bonus was **NULL → IFNULL()** makes it **0** → **matches the condition**.
# MAGIC   - If Bonus was **0** → **matches the condition**.
# MAGIC   - Any **non-zero** Bonus values are **excluded**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tblEmployeesIsNull 
# MAGIC WHERE ifnull(Remarks, '') = '';

# COMMAND ----------

# MAGIC %md
# MAGIC **IFNULL(Remarks, '')**
# MAGIC
# MAGIC - It **checks** if the **Remarks** value is **NULL**.
# MAGIC - If **Remarks** is **NULL**, it returns **Empty value ('')** instead.
# MAGIC - If **Remarks** is **NOT NULL**, it returns the **actual Remarks value**.
# MAGIC
# MAGIC **= ''**
# MAGIC
# MAGIC - This **filters** the **rows** so that:
# MAGIC   - If Remarks was **NULL → IFNULL()** makes it **Empty value ('')** → **matches the condition**.
# MAGIC   - If Remarks was **Empty value ('')** → **matches the condition**.
# MAGIC   - Any **non-zero** Remarks values are **excluded**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) NOT ISNULL() Function

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tblEmployeesIsNull 
# MAGIC WHERE NOT IFNULL(Remarks, '') = '';

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) LEN()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM StudentsInfo 
# MAGIC WHERE LEN(Remarks) = 0;

# COMMAND ----------

# MAGIC %md
# MAGIC - **LEN(Remarks)** returns the **number of characters** in the Remarks column (**excluding trailing spaces**).
# MAGIC - **= 0** means we are filtering rows where the Remarks column is an **empty string ('')**, i.e., it has **zero characters**.
# MAGIC - This will not match NULL values. because **LEN(NULL)** returns **NULL**, and **NULL = 0 is false**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tblEmployeesIsNull 
# MAGIC WHERE LEN(Remarks) = 0;