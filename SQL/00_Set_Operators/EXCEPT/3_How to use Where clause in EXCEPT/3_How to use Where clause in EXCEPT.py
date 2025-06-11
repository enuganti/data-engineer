# Databricks notebook source
# MAGIC %md
# MAGIC **1) Using WHERE clause in Except**
# MAGIC
# MAGIC **a) How to use Except operator on a single table?**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Except_tblSingleEmployees;
# MAGIC
# MAGIC Create table Except_tblSingleEmployees
# MAGIC (
# MAGIC  EmployeeID int,
# MAGIC  Name VARCHAR(40),
# MAGIC  Gender VARCHAR(5),
# MAGIC  Salary INT,
# MAGIC  Category VARCHAR(20)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO Except_tblSingleEmployees VALUES (101, 'Murali', 'M', 52000, "High");
# MAGIC INSERT INTO Except_tblSingleEmployees VALUES (102, 'Stephy', 'F', 55000, "Low");
# MAGIC INSERT INTO Except_tblSingleEmployees VALUES (103, 'Senthil', 'M', 45000, "Medium");
# MAGIC INSERT INTO Except_tblSingleEmployees VALUES (104, 'Janardan', 'M', 40000, "Average");
# MAGIC INSERT INTO Except_tblSingleEmployees VALUES (105, 'Sandhya', 'F', 48000, "Low");
# MAGIC INSERT INTO Except_tblSingleEmployees VALUES (106, 'Vasavi', 'F', 60000, "High");
# MAGIC INSERT INTO Except_tblSingleEmployees VALUES (107, 'Tarun', 'M', 58000, "Medium");
# MAGIC INSERT INTO Except_tblSingleEmployees VALUES (108, 'Ganapathy', 'M', 65000, "Average");
# MAGIC INSERT INTO Except_tblSingleEmployees VALUES (109, 'Trisha', 'F', 67000, "High");
# MAGIC INSERT INTO Except_tblSingleEmployees VALUES (110, 'Balu', 'M', 80000, "Low");
# MAGIC
# MAGIC SELECT * FROM Except_tblSingleEmployees ORDER BY EmployeeID;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT EmployeeID, Name, Gender, Salary, Category 
# MAGIC FROM Except_tblSingleEmployees
# MAGIC WHERE Salary >= 50000
# MAGIC EXCEPT
# MAGIC SELECT EmployeeID, Name, Gender, Salary, Category
# MAGIC FROM Except_tblSingleEmployees
# MAGIC WHERE Salary >= 60000
# MAGIC ORDER BY EmployeeID;

# COMMAND ----------

# MAGIC %md
# MAGIC **b) EXCEPT with BETWEEN Operator**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_students;
# MAGIC
# MAGIC CREATE TABLE tbl_students(
# MAGIC    ID INT NOT NULL, 
# MAGIC    NAME VARCHAR(20) NOT NULL, 
# MAGIC    SUBJECT VARCHAR(20) NOT NULL, 
# MAGIC    AGE INT NOT NULL, 
# MAGIC    HOBBY VARCHAR(20) NOT NULL
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tbl_students VALUES
# MAGIC (1, 'Naina', 'Maths', 24, 'Cricket'),
# MAGIC (2, 'Varun', 'Physics', 26, 'Football'),
# MAGIC (3, 'Dev', 'Maths', 23, 'Cricket'),
# MAGIC (4, 'Priya', 'Physics', 25, 'Cricket'),
# MAGIC (5, 'Aditya', 'Chemistry', 21, 'Cricket'),
# MAGIC (6, 'Kalyan', 'Maths', 30, 'Football'),
# MAGIC (7, 'Aditya', 'Chemistry', 21, 'Cricket'),
# MAGIC (8, 'Kalyan', 'Chemistry', 32, 'Cricket');
# MAGIC
# MAGIC SELECT * FROM tbl_students;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_students_hobby;
# MAGIC
# MAGIC CREATE TABLE tbl_students_hobby(
# MAGIC    ID INT NOT NULL, 
# MAGIC    NAME VARCHAR(20) NOT NULL, 
# MAGIC    HOBBY VARCHAR(20) NOT NULL, 
# MAGIC    AGE INT NOT NULL
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tbl_students_hobby VALUES
# MAGIC (1, 'Vijay', 'Cricket', 18),
# MAGIC (2, 'Varun', 'Football', 26),
# MAGIC (3, 'Surya', 'Cricket', 19),
# MAGIC (4, 'Karthik', 'Cricket', 25),
# MAGIC (5, 'Sunny', 'Football', 26),
# MAGIC (6, 'Dev', 'Cricket', 23);
# MAGIC
# MAGIC SELECT * FROM tbl_students_hobby;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NAME, HOBBY, AGE
# MAGIC FROM tbl_students
# MAGIC WHERE AGE BETWEEN 20 AND 30
# MAGIC EXCEPT 
# MAGIC SELECT NAME, HOBBY, AGE 
# MAGIC FROM tbl_students_hobby
# MAGIC WHERE AGE BETWEEN 20 AND 30

# COMMAND ----------

# MAGIC %md
# MAGIC **Ex 02**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_Employees_India;
# MAGIC
# MAGIC Create table tbl_Employees_India
# MAGIC (
# MAGIC  ID int,
# MAGIC  Name VARCHAR(40),
# MAGIC  Department VARCHAR(5)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tbl_Employees_India VALUES (1, 'Raj', 'Sales');
# MAGIC INSERT INTO tbl_Employees_India VALUES (2, 'Priya', 'HR');
# MAGIC INSERT INTO tbl_Employees_India VALUES (3, 'Akash', 'IT');
# MAGIC INSERT INTO tbl_Employees_India VALUES (4, 'Sneha', 'Sales');
# MAGIC
# MAGIC SELECT * FROM tbl_Employees_India;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_Employees_USA;
# MAGIC
# MAGIC Create table tbl_Employees_USA
# MAGIC (
# MAGIC  ID int,
# MAGIC  Name VARCHAR(40),
# MAGIC  Department VARCHAR(15)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tbl_Employees_USA VALUES (2, 'Priya', 'HR');
# MAGIC INSERT INTO tbl_Employees_USA VALUES (3, 'Akash', 'IT');
# MAGIC INSERT INTO tbl_Employees_USA VALUES (5, 'John', 'Marketing');
# MAGIC
# MAGIC SELECT * FROM tbl_Employees_USA;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC WITH OnlyIndia AS (
# MAGIC     SELECT ID, Name FROM tbl_Employees_India
# MAGIC     EXCEPT
# MAGIC     SELECT ID, Name FROM tbl_Employees_USA
# MAGIC )
# MAGIC SELECT * FROM OnlyIndia
# MAGIC WHERE Name LIKE 'S%';