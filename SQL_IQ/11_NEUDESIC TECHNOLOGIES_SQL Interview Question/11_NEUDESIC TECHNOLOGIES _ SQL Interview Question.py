# Databricks notebook source
# MAGIC %md
# MAGIC **COMPANY: NEUDESIC TECHNOLOGIES**

# COMMAND ----------

# MAGIC %md
# MAGIC **Question**
# MAGIC - In this puzzle you have to Generate a **new column** called **Nums**(Number to call).The conditions to generate the new column (Nums) are-
# MAGIC
# MAGIC   - If the **MobileNum** is **not null** then take the value from the **MobileNum** and **append ‘MobileNum’** to the data.
# MAGIC
# MAGIC   - If the **MobileNum** is **null** then take the value from the **ResidenceNum**(if available)and **append ‘ResidenceNum’** to the data.
# MAGIC
# MAGIC   - If both **MobileNum and ResidenceNum** are **NOT NULL** then take the value from the **MobileNum** and **append ‘MobileNum’** to the data.
# MAGIC
# MAGIC   - If both **MobileNum and ResidenceNum** are **NULL** then we should get **NULL** as output.
# MAGIC
# MAGIC   - The challenge is to do this **without USING CASE Statement**.
# MAGIC
# MAGIC - For more details please check the sample input and expected output.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Sample Input:**
# MAGIC
# MAGIC | Id   | Name     | MobileNum     | ResidenceNum |
# MAGIC |------|----------|---------------|--------------|
# MAGIC |1     | Joey     | 9762575422    |    NULL      |
# MAGIC |2     | Ross     | 9987796244    |  8762575402  |
# MAGIC |3     | Chandler |   NULL        |  7645764689  |
# MAGIC |4     | Monica   | 8902567839    |  7825367901  |
# MAGIC |5     | Rachel   |   NULL        |  7845637289  |
# MAGIC |6     | Pheobe   | 9872435789    |  9838653469  |
# MAGIC |7     | Gunther  |   NULL        |    NULL      |
# MAGIC |8     | Mike     |   NULL        |  9700103678  |
# MAGIC
# MAGIC **Sample Output:**
# MAGIC
# MAGIC | Id   |  Name       |            Nums             |
# MAGIC |------|-------------|-----------------------------|
# MAGIC | 1    |  Joey       |   9762575422 MobileNum      |
# MAGIC | 2    |  Ross       |   9987796244 MobileNum      |
# MAGIC | 3    |  Chandler   |   7645764689 ResidenceNum   |
# MAGIC | 4    |  Monica     |   8902567839 MobileNum      |
# MAGIC | 5    |  Rachel     |   7845637289 ResidenceNum   |
# MAGIC | 6    |  Pheobe     |   9872435789 MobileNum      |
# MAGIC | 7    |  Gunther    |   NULL                      |
# MAGIC | 8    |  Mike       |   9700103678 ResidenceNum   |

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS PhoneNumbers;
# MAGIC
# MAGIC CREATE TABLE PhoneNumbers
# MAGIC (Id INT,
# MAGIC  Name VARCHAR(10),
# MAGIC  MobileNum VARCHAR(100),
# MAGIC  ResidenceNum VARCHAR(100)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO PhoneNumbers
# MAGIC VALUES
# MAGIC (1, 'Joey', '9762575422', NULL),
# MAGIC (2, 'Ross', '9987796244', '8762575402'),
# MAGIC (3, 'Chandler', NULL, '7645764689'),
# MAGIC (4, 'Monica', '8902567839', '7825367901'),
# MAGIC (5, 'Rachel', NULL, '7845637289'),
# MAGIC (6, 'Pheobe', '9872435789', '9838653469'),
# MAGIC (7, 'Gunther', NULL, NULL),
# MAGIC (8, 'Mike', NULL, '9700103678');
# MAGIC
# MAGIC SELECT * FROM PhoneNumbers

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     ID, NAME,
# MAGIC     CONCAT(MobileNum, ' MobileNum') AS Concat_MobileNum,
# MAGIC     CONCAT(ResidenceNum, ' ResidenceNum') AS Concat_ResidenceNum,
# MAGIC     COALESCE(
# MAGIC         CONCAT(MobileNum, ' MobileNum'), 
# MAGIC         CONCAT(ResidenceNum, ' ResidenceNum'),
# MAGIC         NULL
# MAGIC     ) AS Concat_MobileNum_ResidenceNum
# MAGIC FROM phonenumbers;

# COMMAND ----------

# MAGIC %md
# MAGIC - **COALESCE** returns the **first non-NULL** value from the list of expressions provided.
# MAGIC
# MAGIC   - Concatenate MobileNum with the string ' MobileNum' (e.g., "12345 MobileNum").
# MAGIC
# MAGIC   - If MobileNum is NULL, it tries ResidenceNum + ' ResidenceNum'.
# MAGIC
# MAGIC   - If both are NULL, it returns NULL.

# COMMAND ----------

# MAGIC %md
# MAGIC **What it does overall:**
# MAGIC
# MAGIC For each record in phonenumbers:
# MAGIC - It shows ID, NAME, and either:
# MAGIC   - **"MobileNum MobileNum"** if **MobileNum is present**,
# MAGIC   - or **"ResidenceNum ResidenceNum"** if **MobileNum is null**,
# MAGIC   - or **NULL** if **both are missing**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 02**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     ID, NAME,
# MAGIC     MobileNum || ' MobileNum' AS Concatenation_MobileNum,
# MAGIC     ResidenceNum || ' ResidenceNum' AS Concatenation_ResidenceNum,
# MAGIC     COALESCE(
# MAGIC         MobileNum || ' MobileNum', 
# MAGIC         ResidenceNum || ' ResidenceNum',
# MAGIC         NULL
# MAGIC     ) AS Coalesce_Concatenation_MobileResidenceNum
# MAGIC FROM phonenumbers;

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 03**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, name,
# MAGIC        COALESCE(mobilenum, residencenum) AS Coalesce_mobile_residence,
# MAGIC        CONCAT(
# MAGIC           COALESCE(mobilenum, residencenum),
# MAGIC           ' ',
# MAGIC           CASE
# MAGIC             WHEN mobilenum <> '' THEN 'MobileNum'
# MAGIC             WHEN residencenum <> '' THEN 'ResidenceNum'
# MAGIC             ELSE NULL
# MAGIC           END
# MAGIC         ) AS nums
# MAGIC FROM phonenumbers;

# COMMAND ----------

# MAGIC %md
# MAGIC - If **mobilenum** is **not an empty string ('')**, it adds **'MobileNum'**.
# MAGIC - Else if **residencenum** is **not an empty string**, it adds **'ResidenceNum'**.
# MAGIC - If **both are empty strings**, it adds **NULL** (so CONCAT may result in just the number or null).