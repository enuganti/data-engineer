# Databricks notebook source
# MAGIC %md
# MAGIC #### How to find which columns in a table have all NULL records?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample table
# MAGIC DROP TABLE IF EXISTS tblRegisteredProducts;
# MAGIC CREATE TABLE tblRegisteredProducts (
# MAGIC     Emp_ID INT,
# MAGIC     Sales_FROM DATE,
# MAGIC     Sales_TO DATE,
# MAGIC     Product_ID VARCHAR(50),
# MAGIC     Emp_Status VARCHAR(20),
# MAGIC     Emp_Text VARCHAR(100),
# MAGIC     Emp_Description VARCHAR(255),
# MAGIC     Sales_ID INT,
# MAGIC     Selection BOOLEAN
# MAGIC );
# MAGIC
# MAGIC -- 2️⃣ Insert sample data
# MAGIC INSERT INTO tblRegisteredProducts VALUES
# MAGIC (NULL, NULL, '2025-03-16', NULL, NULL, NULL, NULL, 10201, NULL),
# MAGIC (NULL, '2021-01-01', '2025-01-31', 'SN123', NULL, 'Active Product', 'Product description', 10101, TRUE),
# MAGIC (NULL, '2025-04-25', NULL, 'SND231', NULL, NULL, 'Terminalogy', NULL, FALSE),
# MAGIC (NULL, '2022-05-11', '2025-01-31', 'SN123', NULL, 'Products', 'Technology', 33101, TRUE),
# MAGIC (NULL, '2025-02-21', NULL, 'SN123', NULL, 'Mandate', 'Division', 37101, TRUE),
# MAGIC (NULL, '2023-03-14', '2024-02-29', NULL, NULL, 'Serial', 'Sub-division', 26101, TRUE),
# MAGIC (NULL, '2024-04-19', '2023-04-22', 'SN123', NULL, 'Auomate', 'HVAC', 78201, TRUE),
# MAGIC (NULL, '2025-06-16', NULL, 'SN123', NULL, NULL, 'Accounts', 98321, TRUE),
# MAGIC (NULL, '2024-07-26', '2025-06-23', NULL, NULL, 'Details', NULL, 76543, TRUE);
# MAGIC
# MAGIC SELECT * FROM tblRegisteredProducts;

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Emp_ID' AS Emp_ID
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM tblRegisteredProducts
# MAGIC   WHERE Emp_ID IS NOT NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1
# MAGIC FROM tblRegisteredProducts
# MAGIC WHERE Emp_ID IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC **NOT EXISTS (...)** checks:
# MAGIC - **True** if **no rows** are returned (meaning **all Emp_ID are NULL**).
# MAGIC - **False** if **at least one row** exists with a **non-null Emp_ID**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Sales_FROM' AS Sales_FROM
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM tblRegisteredProducts
# MAGIC   WHERE Sales_FROM IS NOT NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1
# MAGIC FROM tblRegisteredProducts
# MAGIC WHERE Sales_FROM IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 02**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE WHEN COUNT(Emp_ID) = 0 THEN 'Emp_ID' END AS Emp_ID,
# MAGIC     CASE WHEN COUNT(Sales_FROM) = 0 THEN 'Sales_FROM' END AS Sales_FROM,
# MAGIC     CASE WHEN COUNT(Sales_TO) = 0 THEN 'Sales_TO' END AS Sales_TO,
# MAGIC     CASE WHEN COUNT(Product_ID) = 0 THEN 'Product_ID' END AS Product_ID,
# MAGIC     CASE WHEN COUNT(Emp_Status) = 0 THEN 'Emp_Status' END AS Emp_Status,
# MAGIC     CASE WHEN COUNT(Emp_Text) = 0 THEN 'Emp_Text' END AS Emp_Text,
# MAGIC     CASE WHEN COUNT(Emp_Description) = 0 THEN 'Emp_Description' END AS Emp_Description,
# MAGIC     CASE WHEN COUNT(Sales_ID) = 0 THEN 'Sales_ID' END AS Sales_ID,
# MAGIC     CASE WHEN COUNT(Selection) = 0 THEN 'Selection' END AS Selection
# MAGIC FROM tblRegisteredProducts;

# COMMAND ----------

# MAGIC %md
# MAGIC - **COUNT(column) ignores NULLs**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(Emp_ID) AS Emp_ID,
# MAGIC        COUNT(Sales_FROM) AS Sales_FROM,
# MAGIC        COUNT(Sales_TO) AS Sales_TO,
# MAGIC        COUNT(Product_ID) AS Product_ID,
# MAGIC        COUNT(Emp_Status) AS Emp_Status,
# MAGIC        COUNT(Emp_Text) AS Emp_Text,
# MAGIC        COUNT(Emp_Description) AS Emp_Description,
# MAGIC        COUNT(Sales_ID) AS Sales_ID,
# MAGIC        COUNT(Selection) AS Selection
# MAGIC FROM tblRegisteredProducts

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 03**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE WHEN COUNT(CASE WHEN Emp_ID IS NOT NULL THEN 1 END) = 0 THEN 'Emp_ID' END AS Emp_ID_status,
# MAGIC     CASE WHEN COUNT(CASE WHEN Sales_FROM IS NOT NULL THEN 1 END) = 0 THEN 'Sales_FROM' END AS Sales_FROM_status,
# MAGIC     CASE WHEN COUNT(CASE WHEN Sales_TO IS NOT NULL THEN 1 END) = 0 THEN 'Sales_TO' END AS Sales_TO_status,
# MAGIC     CASE WHEN COUNT(CASE WHEN Product_ID IS NOT NULL THEN 1 END) = 0 THEN 'Product_ID' END AS Product_ID_status,
# MAGIC     CASE WHEN COUNT(CASE WHEN Emp_Status IS NOT NULL THEN 1 END) = 0 THEN 'Emp_Status' END AS Emp_Status_status,
# MAGIC     CASE WHEN COUNT(CASE WHEN Emp_Text IS NOT NULL THEN 1 END) = 0 THEN 'Emp_Text' END AS Emp_Text_status,
# MAGIC     CASE WHEN COUNT(CASE WHEN Emp_Description IS NOT NULL THEN 1 END) = 0 THEN 'Emp_Description' END AS Emp_Description_status,
# MAGIC     CASE WHEN COUNT(CASE WHEN Sales_ID IS NOT NULL THEN 1 END) = 0 THEN 'Sales_ID' END AS Sales_ID_status,
# MAGIC     CASE WHEN COUNT(CASE WHEN Selection IS NOT NULL THEN 1 END) = 0 THEN 'Selection' END AS Selection_status
# MAGIC FROM tblRegisteredProducts;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN Emp_ID IS NOT NULL THEN 1 END AS Emp_ID
# MAGIC FROM tblRegisteredProducts;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(CASE WHEN Emp_ID IS NOT NULL THEN 1 END) AS Emp_ID
# MAGIC FROM tblRegisteredProducts;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN Sales_FROM IS NOT NULL THEN 1 END AS Sales_From
# MAGIC FROM tblRegisteredProducts;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(CASE WHEN Sales_FROM IS NOT NULL THEN 1 END) AS Sales_From
# MAGIC FROM tblRegisteredProducts;

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 04**
# MAGIC   - It tries to check whether the entire column **Emp_ID** contains **only NULLs**.
# MAGIC   - If **yes**, it returns the string **Emp_ID**.
# MAGIC
# MAGIC   - **COUNT(CASE WHEN Emp_ID IS NOT NULL THEN 1 END)** counts how many **non-null** values are in the **Emp_ID** column.
# MAGIC
# MAGIC   - The query checks whether this **count is equal to 0** i.e., **no non-null values** exist in the **Emp_ID** column.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Emp_ID' AS Emp_ID
# MAGIC FROM tblRegisteredProducts
# MAGIC HAVING COUNT(CASE WHEN Emp_ID IS NOT NULL THEN 1 END) = 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Sales_FROM' AS Sales_FROM
# MAGIC FROM tblRegisteredProducts
# MAGIC HAVING COUNT(CASE WHEN Sales_FROM IS NOT NULL THEN 1 END) = 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Sales_TO' AS column_name
# MAGIC FROM tblRegisteredProducts
# MAGIC HAVING COUNT(CASE WHEN Sales_TO IS NOT NULL THEN 1 END) = 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Product_ID' AS Product_ID
# MAGIC FROM tblRegisteredProducts
# MAGIC HAVING COUNT(CASE WHEN Product_ID IS NOT NULL THEN 1 END) = 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Emp_Status' AS Emp_Status
# MAGIC FROM tblRegisteredProducts
# MAGIC HAVING COUNT(CASE WHEN Emp_Status IS NOT NULL THEN 1 END) = 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Emp_Text' AS Emp_Text
# MAGIC FROM tblRegisteredProducts
# MAGIC HAVING COUNT(CASE WHEN Emp_Text IS NOT NULL THEN 1 END) = 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Emp_Description' AS Emp_Description
# MAGIC FROM tblRegisteredProducts
# MAGIC HAVING COUNT(CASE WHEN Emp_Description IS NOT NULL THEN 1 END) = 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Sales_ID' AS Sales_ID
# MAGIC FROM tblRegisteredProducts
# MAGIC HAVING COUNT(CASE WHEN Sales_ID IS NOT NULL THEN 1 END) = 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Selection' AS Selection
# MAGIC FROM tblRegisteredProducts
# MAGIC HAVING COUNT(CASE WHEN Selection IS NOT NULL THEN 1 END) = 0;