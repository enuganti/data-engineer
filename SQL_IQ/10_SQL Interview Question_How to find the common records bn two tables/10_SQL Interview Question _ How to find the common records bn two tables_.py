# Databricks notebook source
# MAGIC %md
# MAGIC **Four ways to find common records**
# MAGIC - INNER JOIN
# MAGIC - INTERSECT
# MAGIC - IN with Subquery
# MAGIC - EXISTS

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Emp_tbl_design;
# MAGIC
# MAGIC CREATE TABLE Emp_tbl_design (
# MAGIC  EMP_ID VARCHAR(20),
# MAGIC  EMP_Name VARCHAR(15),
# MAGIC  EMP_Salary INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO Emp_tbl_design
# MAGIC VALUES
# MAGIC ("ELX001", "Naresh", 20000),
# MAGIC ("ELX002", "Kumar", 25000),
# MAGIC ("ELX003", "Rahul", 29000);
# MAGIC
# MAGIC SELECT * FROM Emp_tbl_design;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Emp_tbl_deal;
# MAGIC
# MAGIC CREATE TABLE Emp_tbl_deal (
# MAGIC  EMP_ID VARCHAR(20),
# MAGIC  EMP_Name VARCHAR(15),
# MAGIC  EMP_Salary INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO Emp_tbl_deal
# MAGIC VALUES
# MAGIC ("ELX001", "Naresh", 20000),
# MAGIC ("ELX003", "Rahul", 29000);
# MAGIC
# MAGIC SELECT * FROM Emp_tbl_deal;

# COMMAND ----------

# MAGIC %md
# MAGIC **1) INTERSECT**
# MAGIC - Best for **Exact Match – Same Columns**
# MAGIC - **Exact full row match**
# MAGIC -  This returns rows that **exist in both tables**, with **matching values** in all selected columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Emp_tbl_design
# MAGIC INTERSECT
# MAGIC SELECT * FROM Emp_tbl_deal;

# COMMAND ----------

# MAGIC %md
# MAGIC **2) INNER JOIN**
# MAGIC - Best for **Common Key Matching**.
# MAGIC - Use this when you're **matching rows** based on **one or more keys/IDs, not full row** content.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT e.EMP_ID, e.EMP_Name, e.EMP_Salary
# MAGIC FROM Emp_tbl_design e
# MAGIC INNER JOIN Emp_tbl_deal d
# MAGIC ON e.EMP_ID = d.EMP_ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **3) IN with Subquery**
# MAGIC - Only **comparing one column**.
# MAGIC - Good for checking **common values** in a **single column**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT EMP_ID FROM Emp_tbl_design
# MAGIC WHERE EMP_ID IN (
# MAGIC   SELECT EMP_ID
# MAGIC   FROM Emp_tbl_deal);

# COMMAND ----------

# MAGIC %md
# MAGIC **4) EXISTS**
# MAGIC - Efficient for **large datasets**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Emp_tbl_design t1
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1 FROM Emp_tbl_deal t2
# MAGIC     WHERE t1.EMP_ID = t2.EMP_ID
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC - The **1** is just a **placeholder value**.
# MAGIC
# MAGIC - It doesn't matter what you select inside an EXISTS clause SQL only checks whether **any row exists**, not what is selected.
# MAGIC
# MAGIC - You could write as below, and it would behave **exactly the same**.
# MAGIC   - SELECT *
# MAGIC   - SELECT EMP_ID
# MAGIC   - SELECT 'x'
# MAGIC   - SELECT 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Summary:**
# MAGIC
# MAGIC | Use Case                        | Best Method  | Supported In           |
# MAGIC | ------------------------------- | ------------ | ---------------------- |
# MAGIC | Exact full row match            | `INTERSECT`  | PostgreSQL, SQL Server |
# MAGIC | Common rows by key/column       | `INNER JOIN` | All (✅ MySQL too)      |
# MAGIC | Common values in 1 column       | `IN`         | All                    |
# MAGIC | Existence check for performance | `EXISTS`     | All                    |