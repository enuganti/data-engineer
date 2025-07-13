# Databricks notebook source
# MAGIC %md
# MAGIC **1) Combine COALESCE with CONCAT**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_coalesce_contacts;
# MAGIC
# MAGIC CREATE TABLE tbl_coalesce_contacts (
# MAGIC     contact_id INT,
# MAGIC     phone_mobile VARCHAR(15),
# MAGIC     phone_home VARCHAR(15)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tbl_coalesce_contacts
# MAGIC VALUES
# MAGIC (1, '9876543210', NULL),
# MAGIC (2, NULL, '0401234567'),
# MAGIC (3, NULL, NULL),
# MAGIC (4, NULL, '080987654'),
# MAGIC (5, '987654321', NULL),
# MAGIC (6, '6789654321', '9887612345');
# MAGIC
# MAGIC SELECT * FROM tbl_coalesce_contacts;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT contact_id, phone_mobile, phone_home,
# MAGIC        CONCAT(phone_mobile, ' (Mobile)') AS Concate_Phone_Mobile,
# MAGIC        CONCAT(phone_home, ' (Home)') AS Concate_Phone_Home,
# MAGIC        COALESCE(
# MAGIC            CONCAT(phone_mobile, ' (Mobile)'),
# MAGIC            CONCAT(phone_home, ' (Home)'),
# MAGIC            'No Phone') AS preferred_contact
# MAGIC FROM tbl_coalesce_contacts;

# COMMAND ----------

# MAGIC %md
# MAGIC **How it works:**
# MAGIC
# MAGIC - **CONCAT()** joins **strings** together.
# MAGIC
# MAGIC - **COALESCE()** returns the **first non-NULL** expression among its arguments.
# MAGIC
# MAGIC - If **phone_mobile is not NULL**, it returns that number labeled as **"(Mobile)"**.
# MAGIC
# MAGIC - If **phone_mobile** is **NULL** but **phone_home** is **available**, it returns that labeled as **"(Home)"**.
# MAGIC
# MAGIC - If **both are NULL**, it returns the string **"No Phone"**.

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 01**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_coalesce_concat_employees;
# MAGIC
# MAGIC -- Create the employees table
# MAGIC CREATE TABLE tbl_coalesce_concat_employees (
# MAGIC     emp_id INT,
# MAGIC     first_name VARCHAR(50),
# MAGIC     last_name VARCHAR(50)
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO tbl_coalesce_concat_employees (emp_id, first_name, last_name)
# MAGIC VALUES
# MAGIC (1, 'Ravi', 'Kumar'),
# MAGIC (2, 'Bindu', NULL),
# MAGIC (3, NULL, 'Smitha'),
# MAGIC (4, NULL, NULL),
# MAGIC (5, 'Bhanu', 'Prakash');
# MAGIC
# MAGIC SELECT * FROM tbl_coalesce_concat_employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query: Combine first_name and last_name using COALESCE and CONCAT
# MAGIC SELECT first_name, last_name,
# MAGIC        COALESCE(first_name, '') AS Coalesce_FirstName,
# MAGIC        COALESCE(last_name, '') AS Coalesce_LastName,
# MAGIC        CONCAT(
# MAGIC          COALESCE(first_name, ''), 
# MAGIC          '_', 
# MAGIC          COALESCE(last_name, '')) AS Coalesce_FirstName_LastName
# MAGIC FROM tbl_coalesce_concat_employees;

# COMMAND ----------

# MAGIC %md
# MAGIC **Explanation:**
# MAGIC
# MAGIC - If **first_name** is **NULL**, it uses **'' (empty string)**.
# MAGIC
# MAGIC - If **last_name** is **NULL**, it also uses **''**.
# MAGIC
# MAGIC - The result is a safe **full name** with **no NULL** in output.

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 02**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_coalesce_concat_contacts;
# MAGIC
# MAGIC -- Create the table
# MAGIC CREATE TABLE tbl_coalesce_concat_contacts (
# MAGIC     contact_id INT,
# MAGIC     phone_mobile VARCHAR(15),
# MAGIC     phone_home VARCHAR(15)
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO tbl_coalesce_concat_contacts (contact_id, phone_mobile, phone_home)
# MAGIC VALUES
# MAGIC (1, '9876543210', NULL),
# MAGIC (2, NULL, '0401234567'),
# MAGIC (3, NULL, NULL),
# MAGIC (4, NULL, '080987654'),
# MAGIC (5, '987654321', NULL);
# MAGIC
# MAGIC SELECT * FROM tbl_coalesce_concat_contacts;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT phone_mobile, phone_home,
# MAGIC        COALESCE(phone_mobile, 'N/A') AS Coalesce_Phone_Mobile,
# MAGIC        COALESCE(phone_home, 'N/A') AS Coalesce_Phone_Home,
# MAGIC        CONCAT(
# MAGIC         'Mobile: ', COALESCE(phone_mobile, 'N/A'),
# MAGIC         ', Home: ', COALESCE(phone_home, 'N/A')) AS contact_info
# MAGIC FROM tbl_coalesce_concat_contacts;

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 03**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_coalesce_concat_addresses;
# MAGIC
# MAGIC -- Create the table
# MAGIC CREATE TABLE tbl_coalesce_concat_addresses (
# MAGIC     address_id INT,
# MAGIC     city VARCHAR(50),
# MAGIC     state VARCHAR(50),
# MAGIC     country VARCHAR(50)
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO tbl_coalesce_concat_addresses (address_id, city, state, country)
# MAGIC VALUES
# MAGIC (1, 'Bangalore', 'Karnataka', 'India'),
# MAGIC (2, NULL, 'California', 'USA'),
# MAGIC (3, 'Toronto', NULL, 'Canada'),
# MAGIC (4, NULL, NULL, NULL),
# MAGIC (5, 'London', 'England', NULL);
# MAGIC
# MAGIC SELECT * FROM tbl_coalesce_concat_addresses;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city, state, country,
# MAGIC        COALESCE(city, 'Unknown City') AS Colaesce_City,
# MAGIC        COALESCE(state, 'Unknown State') AS Colaesce_State,
# MAGIC        COALESCE(country, 'Unknown Country') AS Colaesce_Country,
# MAGIC        CONCAT_WS(
# MAGIC          ' - ',
# MAGIC          COALESCE(city, 'Unknown City'),
# MAGIC          COALESCE(state, 'Unknown State'),
# MAGIC          COALESCE(country, 'Unknown Country')) AS location
# MAGIC FROM tbl_coalesce_concat_addresses;

# COMMAND ----------

# MAGIC %md
# MAGIC - **CONCAT_WS(separator, ...) skips NULLs** automatically.
# MAGIC
# MAGIC - But if you still want **default values**, use **COALESCE**.