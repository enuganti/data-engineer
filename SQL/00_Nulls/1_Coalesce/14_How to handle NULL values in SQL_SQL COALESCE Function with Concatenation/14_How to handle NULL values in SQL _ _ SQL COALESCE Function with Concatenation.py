# Databricks notebook source
# MAGIC %md
# MAGIC #### a) concatenation operator: ||.
# MAGIC   - It allows us to **merge two or more strings** into a **single output**.
# MAGIC   - Combines **multiple columns or strings** into **one**.
# MAGIC   - Works with **different data types** (text, numbers, dates).

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      SELECT column1 || column2 AS new_column
# MAGIC      FROM table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Concatenating First Name and Last Name**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tblConcatenateEmployees;
# MAGIC
# MAGIC -- Create the table
# MAGIC CREATE TABLE tblConcatenateEmployees (
# MAGIC     ID INT,
# MAGIC     First_Name VARCHAR(50),
# MAGIC     Last_Name VARCHAR(50),
# MAGIC     Salary INT,
# MAGIC     Age INT
# MAGIC );
# MAGIC
# MAGIC -- Insert data
# MAGIC INSERT INTO tblConcatenateEmployees (ID, First_Name, Last_Name, Salary, Age)
# MAGIC VALUES
# MAGIC (1, 'Rajesh', 'Kumar', 16500, 25),
# MAGIC (2, 'Gopinath', 'Munday', 25600, 28),
# MAGIC (3, 'Shekar', 'Varma', 59800, 32),
# MAGIC (4, 'Karishma', 'Yadav', 956400, 35),
# MAGIC (5, 'Kamlesh', 'Sharma', 75800, 29),
# MAGIC (6, 'Joshna', 'Kumari', 257800, 24),
# MAGIC (7, NULL, 'Paul', 257800, 24),
# MAGIC (8, 'Joshna', NULL, 257800, 24);
# MAGIC
# MAGIC SELECT * FROM tblConcatenateEmployees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id,
# MAGIC        First_Name, Last_Name,
# MAGIC        First_Name || Last_Name AS Full_Name_FirstLastName,
# MAGIC        Salary
# MAGIC FROM tblConcatenateEmployees;

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Concatenating Strings with a Literal**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, First_Name, Last_Name, Salary, 
# MAGIC        First_Name||' has salary of '||Salary AS New_Column
# MAGIC FROM tblConcatenateEmployees;

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Using a Number Literal in Concatenation**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, First_Name, Last_Name, Salary,
# MAGIC        First_Name || '_' || 100 || ' has ID Number of ' || id AS NewIDColumn
# MAGIC FROM tblConcatenateEmployees;

# COMMAND ----------

# MAGIC %md
# MAGIC **Difference b/n concat and concatenation (||)**
# MAGIC
# MAGIC      ('John', NULL)
# MAGIC
# MAGIC      # using concat
# MAGIC      CONCAT('John', NULL) → 'John'
# MAGIC      
# MAGIC      # Using || (concatenation)
# MAGIC      'John' || NULL → NULL
# MAGIC
# MAGIC      # best practice
# MAGIC      SELECT ID, FIRST_NAME, LAST_NAME,
# MAGIC             COALESCE(FIRST_NAME, '') || COALESCE(LAST_NAME, '') AS FULL_NAME,
# MAGIC            SALARY
# MAGIC      FROM tblConcatenateEmployees;

# COMMAND ----------

# MAGIC %md
# MAGIC #### b) Coalesce with Concatenation

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_concatenation_contacts;
# MAGIC
# MAGIC CREATE TABLE tbl_concatenation_contacts (
# MAGIC     contact_id INT,
# MAGIC     phone_mobile VARCHAR(15),
# MAGIC     phone_home VARCHAR(15)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tbl_concatenation_contacts
# MAGIC VALUES
# MAGIC (1, '9876543210', NULL),
# MAGIC (2, NULL, '0401234567'),
# MAGIC (3, NULL, NULL),
# MAGIC (4, NULL, '080987654'),
# MAGIC (5, '987654321', NULL),
# MAGIC (6, '6789654321', '9887612345');
# MAGIC
# MAGIC SELECT * FROM tbl_concatenation_contacts;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     phone_mobile, phone_home,
# MAGIC     phone_mobile || ' (Mobile)' AS Phone_Mobile_Concatenation,
# MAGIC     phone_home || ' (Home)' AS Phone_Home_Concatenation,
# MAGIC     COALESCE(
# MAGIC         phone_mobile || ' (Mobile)', 
# MAGIC         phone_home || ' (Home)',
# MAGIC         'No Phone'
# MAGIC     ) AS Comb_Phone_Mobile_Home
# MAGIC FROM tbl_concatenation_contacts;

# COMMAND ----------

# MAGIC %md
# MAGIC - If **phone_mobile** exists, **append ' (Mobile)'**.
# MAGIC - If **not**, use **phone_home** with **' (Home)'**.
# MAGIC - If **both are NULL**, show **'No Phone'**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT contact_id, phone_mobile,
# MAGIC        COALESCE(phone_mobile, '') AS Coalesce_Phone_Mobile,
# MAGIC        COALESCE(phone_mobile, '') || ' Mobile' AS Concat_Phone_Mobile
# MAGIC FROM tbl_concatenation_contacts;

# COMMAND ----------

# MAGIC %md
# MAGIC - Labels a **mobile number** or shows just **' Mobile'** if **NULL**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT phone_mobile, phone_home,
# MAGIC        COALESCE(phone_mobile, '') AS Colaesce_Phone_Mobile,
# MAGIC        COALESCE(phone_home, '') AS Colaesce_Phone_Home,
# MAGIC        COALESCE(phone_mobile, '') || ' / ' || COALESCE(phone_home, '') AS Comb_Phone_Mobile_Home
# MAGIC FROM tbl_concatenation_contacts;