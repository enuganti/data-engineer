# Databricks notebook source
# MAGIC %md
# MAGIC #### How to find the number of rows where all columns are NULL?
# MAGIC
# MAGIC | Name	| department | Customer_ID | Change_Date | Load_Date | Status	| description |
# MAGIC |-------|------------|-------------|-------------|-----------|--------|-------------|
# MAGIC | null	| null	     |   null      |	null	     |  null     |  null  |   null      |
# MAGIC | null	| null	     |   null      |	null	     |  null     |  null  |   null      |
# MAGIC | null	| null	     |   null      |	null	     |  null     |  null  |   null      |
# MAGIC | null	| null	     |   null      |	null	     |  null     |  null  |   null      |
# MAGIC | null	| null	     |   null      |	null	     |  null     |  null  |   null      |

# COMMAND ----------

# DBTITLE 1,Sample Dataframe
# MAGIC %sql
# MAGIC -- Create table
# MAGIC DROP TABLE IF EXISTS tblNullsBlankRows;
# MAGIC CREATE TABLE tblNullsBlankRows (
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
# MAGIC INSERT INTO tblNullsBlankRows VALUES
# MAGIC ('Jagadish', 'HR', 101, '2024-01-05', '2024-01-06', 'Active', 'Employee record'),
# MAGIC ('Ashwin', 'Admin', 102, '2024-04-05', '2024-03-06', 'Default', 'Employee Obsent'),
# MAGIC ('Swarna', 'Finance', 103, '2024-05-05', '2024-04-06', 'Deactivate', 'Products'),
# MAGIC (NULL, NULL, NULL, NULL, NULL, NULL, NULL),
# MAGIC ('Swapna', 'IT', 104, '2024-08-10', '2024-05-12', 'Inactive', 'Resigned'),
# MAGIC ('Praveen', 'Accounts', 105, '2024-09-05', '2024-03-06', 'Active', 'Description'),
# MAGIC (NULL, NULL, NULL, NULL, NULL, NULL, NULL),
# MAGIC ('Bibin', 'Finance', 105, '2024-10-01', '2024-09-02', 'Active', 'Salary details'),
# MAGIC ('Bharath', 'HR', 106, '2024-12-05', '2024-09-06', 'Default', 'Sales Details'),
# MAGIC (NULL, NULL, NULL, NULL, NULL, NULL, NULL),
# MAGIC ('Carl', 'Admin', 104, '2024-04-11', '2024-04-15', 'Active', 'Shift change'),
# MAGIC ('Joseph', 'Developer', 101, '2024-01-05', '2024-01-06', 'Active', 'Employee record'),
# MAGIC (NULL, NULL, NULL, NULL, NULL, NULL, NULL),
# MAGIC ('Damu', 'Sales', 105, '2024-05-18', '2024-05-20', 'Inactive', 'Transferred'),
# MAGIC ('Josna', 'Transport', 101, '2024-01-05', '2024-01-06', 'Active', 'Unlock offers'),
# MAGIC (NULL, NULL, NULL, NULL, NULL, NULL, NULL),
# MAGIC ('Kiran', 'Domestic', 101, '2024-12-05', '2025-05-06', 'Inactive', 'Savings');
# MAGIC
# MAGIC SELECT * FROM tblNullsBlankRows;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Using WHERE with AND conditions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tblNullsBlankRows
# MAGIC WHERE Name IS NULL AND
# MAGIC       department IS NULL AND
# MAGIC       Customer_ID IS NULL AND
# MAGIC       Change_Date IS NULL AND
# MAGIC       Load_Date IS NULL AND
# MAGIC       Status IS NULL AND
# MAGIC       description IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS all_null_rows
# MAGIC FROM tblNullsBlankRows
# MAGIC WHERE Name IS NULL AND
# MAGIC       department IS NULL AND
# MAGIC       Customer_ID IS NULL AND
# MAGIC       Change_Date IS NULL AND
# MAGIC       Load_Date IS NULL AND
# MAGIC       Status IS NULL AND
# MAGIC       description IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Using CASE inside SUM()
# MAGIC - 1 = NULL
# MAGIC - 0 = NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN Name IS NULL 
# MAGIC             AND department IS NULL 
# MAGIC             AND Customer_ID IS NULL 
# MAGIC             AND Change_Date IS NULL 
# MAGIC             AND Load_Date IS NULL 
# MAGIC             AND Status IS NULL 
# MAGIC             AND description IS NULL
# MAGIC         THEN 1 
# MAGIC         ELSE 0 
# MAGIC     END AS all_null_rows
# MAGIC FROM tblNullsBlankRows;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(
# MAGIC     CASE WHEN Name IS NULL
# MAGIC           AND department IS NULL
# MAGIC           AND Customer_ID IS NULL
# MAGIC           AND Change_Date IS NULL
# MAGIC           AND Load_Date IS NULL
# MAGIC           AND Status IS NULL
# MAGIC           AND description IS NULL
# MAGIC     THEN 1 ELSE 0 END
# MAGIC ) AS all_null_rows
# MAGIC FROM tblNullsBlankRows;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Using COALESCE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The COALESCE function requires all arguments to be of the same data type.
# MAGIC -- You need to ensure that all columns passed to COALESCE are of the same type.
# MAGIC SELECT *
# MAGIC FROM tblNullsBlankRows
# MAGIC WHERE COALESCE(
# MAGIC       Name,
# MAGIC       department,
# MAGIC       CAST(Customer_ID AS STRING),
# MAGIC       CAST(Change_Date AS STRING),
# MAGIC       CAST(Load_Date AS STRING),
# MAGIC       Status,
# MAGIC       description
# MAGIC ) IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The COALESCE function requires all arguments to be of the same data type.
# MAGIC -- You need to ensure that all columns passed to COALESCE are of the same type.
# MAGIC SELECT COUNT(*) AS all_null_rows
# MAGIC FROM tblNullsBlankRows
# MAGIC WHERE COALESCE(
# MAGIC       Name,
# MAGIC       department,
# MAGIC       CAST(Customer_ID AS STRING),
# MAGIC       CAST(Change_Date AS STRING),
# MAGIC       CAST(Load_Date AS STRING),
# MAGIC       Status,
# MAGIC       description
# MAGIC ) IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC - **COALESCE(col1, col2, …)** returns the **first non-null value**.
# MAGIC - If **all columns** are **NULL**, the result is **NULL**.
# MAGIC - **WHERE COALESCE(...) IS NULL** ensures we **only count rows** where **all columns are NULL**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Using a derived column (CASE) to check row emptiness

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC          CASE 
# MAGIC            WHEN Name IS NULL
# MAGIC                 AND department IS NULL
# MAGIC                 AND Customer_ID IS NULL
# MAGIC                 AND Change_Date IS NULL
# MAGIC                 AND Load_Date IS NULL
# MAGIC                 AND Status IS NULL
# MAGIC                 AND description IS NULL
# MAGIC            THEN 1 ELSE 0 END AS is_all_null
# MAGIC   FROM tblNullsBlankRows
# MAGIC ) t
# MAGIC WHERE is_all_null = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS all_null_rows
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC          CASE 
# MAGIC            WHEN Name IS NULL
# MAGIC                 AND department IS NULL
# MAGIC                 AND Customer_ID IS NULL
# MAGIC                 AND Change_Date IS NULL
# MAGIC                 AND Load_Date IS NULL
# MAGIC                 AND Status IS NULL
# MAGIC                 AND description IS NULL
# MAGIC            THEN 1 ELSE 0 END AS is_all_null
# MAGIC   FROM tblNullsBlankRows
# MAGIC ) t
# MAGIC WHERE is_all_null = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT *,
# MAGIC          CASE 
# MAGIC            WHEN Name IS NULL
# MAGIC                 AND department IS NULL
# MAGIC                 AND Customer_ID IS NULL
# MAGIC                 AND Change_Date IS NULL
# MAGIC                 AND Load_Date IS NULL
# MAGIC                 AND Status IS NULL
# MAGIC                 AND description IS NULL
# MAGIC            THEN 1 ELSE 0 END AS is_all_null
# MAGIC   FROM tblNullsBlankRows

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) Dynamic method

# COMMAND ----------

from pyspark.sql.functions import col

# Load the table into a DataFrame
df = spark.table("tblNullsBlankRows")

# Generate the condition for checking all columns for NULL values
condition = " AND ".join([f"{col} IS NULL" for col in df.columns])

# Create the SQL query
sql_query = f"SELECT * FROM tblNullsBlankRows WHERE {condition}"
# sql_query = f"SELECT COUNT(*) AS all_null_rows FROM tblNullsBlankRows WHERE {condition}"

# Execute the query
result_df = spark.sql(sql_query)

# Display the result
display(result_df)