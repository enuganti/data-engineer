# Databricks notebook source
# MAGIC %md
# MAGIC ##### NULLIF()
# MAGIC - The **NULLIF()** function returns **NULL** if **two expressions are equal**, otherwise it returns the **first expression**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Syntax
# MAGIC
# MAGIC      NULLIF(expr1, expr2)
# MAGIC
# MAGIC      NULL if expr1 = expr2
# MAGIC      expr1 if expr1 ≠ expr2

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Basic Comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare two expressions
# MAGIC SELECT NULLIF(10, 10) AS result1,                          -- returns NULL  (since 10 = 10)
# MAGIC        NULLIF(10, 20) AS result2,                          -- returns 10    (since 10 ≠ 20)
# MAGIC        NULLIF('Hello', 'Hello') AS result3,                -- returns NULL  (since Hello = Hello)
# MAGIC        NULLIF('Hello', 'world') AS result4,                -- returns Hello (since Hello ≠ world)
# MAGIC        NULLIF('2017-08-25', '2017-08-25') AS result5;      -- returns NULL  (since 2017-08-25 = 2017-08-25)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Avoiding Divide by Zero

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tblEmployeesNullIF;
# MAGIC CREATE TABLE tblEmployeesNullIF (
# MAGIC     EmpID INT PRIMARY KEY,
# MAGIC     Name VARCHAR(50),
# MAGIC     Salary INT,
# MAGIC     YearsWorked INT,
# MAGIC     department VARCHAR(10),
# MAGIC     bonus INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tblEmployeesNullIF (EmpID, Name, Salary, YearsWorked, department, bonus)
# MAGIC VALUES
# MAGIC (1, 'Albert', 50000, 5, 'HR', 5),     -- Normal case
# MAGIC (2, 'Bobby', 60000, 0, 'ADMIN', 0),       -- Divide by zero avoided
# MAGIC (3, 'Charan', 45000, 9, 'FINACE', 6),   -- Normal case
# MAGIC (4, 'David', 30000, NULL, 'HR', 9),  -- NULL YearsWorked
# MAGIC (5, 'Eshwar', 75000, 15, 'ACCOUNTS', 4);      -- Normal case
# MAGIC
# MAGIC SELECT * FROM tblEmployeesNullIF;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Salary / YearsWorked AS AvgPerYear
# MAGIC FROM tblEmployeesNullIF;

# COMMAND ----------

# MAGIC %md
# MAGIC - If **YearsWorked = 0**, this causes an **error**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Salary, YearsWorked,
# MAGIC        Salary / NULLIF(YearsWorked, 0) AS AvgPerYear
# MAGIC FROM tblEmployeesNullIF;

# COMMAND ----------

# MAGIC %md
# MAGIC - If **YearsWorked = 0**, **NULLIF(YearsWorked, 0) = NULL**, so the **result = NULL instead of error**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Removing Duplicate Values

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT EmpID, Name, Department,
# MAGIC        NULLIF(Department, 'HR') AS DeptCheck
# MAGIC FROM tblEmployeesNullIF;

# COMMAND ----------

# MAGIC %md
# MAGIC - If **Department = 'HR'**, returns **NULL**.
# MAGIC - Otherwise, returns **actual Department**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Using in CASE Expressions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT EmpID, Salary, bonus,
# MAGIC        CASE 
# MAGIC             WHEN NULLIF(bonus, 0) IS NULL THEN 'No Bonus'
# MAGIC             ELSE 'Has Bonus'
# MAGIC        END AS BonusStatus
# MAGIC FROM tblEmployeesNullIF;

# COMMAND ----------

# MAGIC %md
# MAGIC - If Bonus = 0, **NULLIF(Bonus,0) = NULL** → "No Bonus".
# MAGIC - Otherwise, **"Has Bonus"**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) Comparing Two Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Salary, bonus,
# MAGIC        NULLIF(Salary, bonus) AS SalaryCheck
# MAGIC FROM tblEmployeesNullIF;

# COMMAND ----------

# MAGIC %md
# MAGIC - If **Salary = Bonus** → returns **NULL**.
# MAGIC - Else → returns **Salary**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6) Filtering Rows

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Name, Department
# MAGIC FROM tblEmployeesNullIF
# MAGIC WHERE NULLIF(Name, Department) IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC - This finds rows where **Name = Department**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7) NULLIF() with COALESCE & AVG

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tblBudgetsNullIf;
# MAGIC CREATE TABLE tblBudgetsNullIf (  
# MAGIC    dept           TINYINT,  
# MAGIC    current_year   DECIMAL(10,2),  
# MAGIC    previous_year  DECIMAL(10,2)  
# MAGIC );  
# MAGIC   
# MAGIC INSERT INTO tblBudgetsNullIf VALUES(1, 100000, 150000);  
# MAGIC INSERT INTO tblBudgetsNullIf VALUES(2, NULL, 300000);  
# MAGIC INSERT INTO tblBudgetsNullIf VALUES(3, 0, 100000);  
# MAGIC INSERT INTO tblBudgetsNullIf VALUES(4, NULL, 150000);  
# MAGIC INSERT INTO tblBudgetsNullIf VALUES(5, 300000, 300000);
# MAGIC
# MAGIC SELECT * FROM tblBudgetsNullIf;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_year, previous_year,
# MAGIC        NULLIF(current_year, previous_year) AS LastBudget  
# MAGIC FROM tblBudgetsNullIf;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_year, previous_year,
# MAGIC        COALESCE(current_year, previous_year) AS Colsc_TotalBudget,
# MAGIC        NULLIF(COALESCE(current_year, previous_year), 0.00) AS NullIF_LastBudget  
# MAGIC FROM tblBudgetsNullIf;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(NULLIF(COALESCE(current_year, previous_year), 0.00)) AS Average_Budget
# MAGIC FROM tblBudgetsNullIf;