# Databricks notebook source
# MAGIC %md
# MAGIC #### How to return rows where at least one column is NULL?
# MAGIC
# MAGIC | OBJECT_ID	| Name     |	department |	Customer_ID	| Change_Date	          | Load_Date   |	Status       |	description	| start_date_source	| 
# MAGIC |-----------|----------|-------------|--------------|-----------------------|-------------|--------------|--------------|-------------------|
# MAGIC | 583069	  | Harish   |	null	     | 13681832     |	2025-05-12T09:25:45Z                  |	2025-06-02  |	Open	       | E-Mail	      | 1724256609000     |
# MAGIC | 506654	  | Basha	   |	null	     | 13681832     | 2025-04-02T04:15:05Z	| 2025-06-02	| Not Relevant | Social Media	| 1724256609000	    |
# MAGIC | 583195	  | null	   | Finance     | 12619703     |	null                  |	2025-06-02	| Started      | Messaging    |	1724256609000     |
# MAGIC | 558253		| null	   | 2269299	   | null	        | 2025-06-02	          | 2025-06-02  | null         | Messaging    | 1724256609000     |
# MAGIC | null	    | Krishna	 | Sales	     | null         |	2025-04-02T06:12:18Z	| 2025-06-02	| null	       | Manual entry	| 1724256609000	    |
# MAGIC | 583181    |	Kiran	   | Marketing	 | 39714449	    | null	                | 2025-06-02	| Finished	   | Other	      | 1724256609000	    |
# MAGIC | 583119	  | Hitesh   |	null       |	10183510	  | 2025-04-02T04:15:13Z	| null	      | Open	       | Telephony    |	1724256609000     |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table
# MAGIC DROP TABLE IF EXISTS tblOneNullsRows;
# MAGIC CREATE TABLE tblOneNullsRows (
# MAGIC     Name VARCHAR(50),
# MAGIC     department VARCHAR(50),
# MAGIC     Customer_ID INT,
# MAGIC     Change_Date DATE,
# MAGIC     Load_Date DATE,
# MAGIC     Status VARCHAR(20),
# MAGIC     description VARCHAR(100)
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO tblOneNullsRows VALUES
# MAGIC ('Jagadish', 'HR', NULL, '2024-01-05', '2024-01-06', 'Active', 'Employee record'),
# MAGIC ('Ashwin', 'Admin', 102, '2024-04-05', '2024-03-06', 'Default', 'Employee Obsent'),
# MAGIC ('Swarna', 'Finance', 103, '2024-05-05', NULL, 'Deactivate', 'Products'),
# MAGIC ('Swapna', 'IT', 104, '2024-08-10', '2024-05-12', 'Inactive', 'Resigned'),
# MAGIC ('Praveen', 'Accounts', 105, '2024-09-05', '2024-03-06', 'Active', 'Description'),
# MAGIC ('Bibin', 'Finance', 105, '2024-10-01', '2024-09-02', 'Active', 'Salary details'),
# MAGIC ('Bharath', NULL, 106, '2024-12-05', '2024-09-06', 'Default', 'Sales Details'),
# MAGIC ('Carl', 'Admin', 104, '2024-04-11', '2024-04-15', 'Active', 'Shift change'),
# MAGIC ('Joseph', 'Developer', 101, '2024-01-05', '2024-01-06', 'Active', 'Employee record'),
# MAGIC ('Damu', 'Sales', 105, '2024-05-18', '2024-05-20', NULL, 'Transferred'),
# MAGIC ('Josna', 'Transport', 101, '2024-01-05', '2024-01-06', 'Active', 'Unlock offers'),
# MAGIC (NULL, 'Domestic', 101, '2024-12-05', '2025-05-06', 'Inactive', 'Savings');
# MAGIC
# MAGIC SELECT * FROM tblOneNullsRows;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Using OR conditions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tblOneNullsRows
# MAGIC WHERE Name IS NULL OR
# MAGIC       department IS NULL OR
# MAGIC       Customer_ID IS NULL OR
# MAGIC       Change_Date IS NULL OR
# MAGIC       Load_Date IS NULL OR
# MAGIC       Status IS NULL OR
# MAGIC       description IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS RowsWithAnyNulls
# MAGIC FROM tblOneNullsRows
# MAGIC WHERE Name IS NULL OR
# MAGIC       department IS NULL OR
# MAGIC       Customer_ID IS NULL OR
# MAGIC       Change_Date IS NULL OR
# MAGIC       Load_Date IS NULL OR
# MAGIC       Status IS NULL OR
# MAGIC       description IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Using CASE()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC     CASE 
# MAGIC         WHEN Name IS NULL 
# MAGIC           OR department IS NULL 
# MAGIC           OR Customer_ID IS NULL 
# MAGIC           OR Change_Date IS NULL 
# MAGIC           OR Load_Date IS NULL 
# MAGIC           OR Status IS NULL 
# MAGIC           OR description IS NULL
# MAGIC         THEN 1 -- 'Has NULL'
# MAGIC         ELSE 0 -- 'No NULL'
# MAGIC     END AS NullCheck
# MAGIC FROM tblOneNullsRows;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Using NOT EXISTS / EXCEPT

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM tblOneNullsRows e
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1 
# MAGIC     WHERE e.Name IS NULL OR
# MAGIC           e.department IS NULL OR
# MAGIC           e.Customer_ID IS NULL OR
# MAGIC           e.Change_Date IS NULL OR
# MAGIC           e.Load_Date IS NULL OR
# MAGIC           e.Status IS NULL OR
# MAGIC           e.description IS NULL
# MAGIC );