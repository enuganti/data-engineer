# Databricks notebook source
# MAGIC %md
# MAGIC - In sql **NULL** is **not equal to NULL**.
# MAGIC - **NULL** value will not also be joined with **NULL** value, because **NULL** is unknown value.
# MAGIC - we cann't join two **NULL's** together.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Types of Join**
# MAGIC
# MAGIC **Inner Join**
# MAGIC
# MAGIC      SELECT * FROM table1 AS t1
# MAGIC      INNER JOIN table2 AS t2 ON t1.ID1=t2.ID1;
# MAGIC
# MAGIC **Left Join**
# MAGIC
# MAGIC      SELECT * FROM table1 AS t1
# MAGIC      LEFT JOIN table2 AS t2 ON t1.ID1=t2.ID1;
# MAGIC
# MAGIC **Right Join**
# MAGIC
# MAGIC      SELECT * FROM table1 AS t1
# MAGIC      RIGHT JOIN table2 AS t2 ON t1.ID1=t2.ID1;
# MAGIC
# MAGIC **Full Outer Join**
# MAGIC
# MAGIC      SELECT * FROM table1 AS t1
# MAGIC      FULL OUTER JOIN table2 AS t2 ON t1.ID1=t2.ID1;
# MAGIC
# MAGIC **Cross Join**
# MAGIC
# MAGIC      SELECT * FROM table1 AS t1
# MAGIC      CROSS JOIN table2 AS t2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Number of Records after join**
# MAGIC
# MAGIC **Inner Join**
# MAGIC
# MAGIC      SELECT COUNT(*) FROM table1 AS t1
# MAGIC      INNER JOIN table2 AS t2 ON t1.ID1=t2.ID1;
# MAGIC
# MAGIC **Left Join**
# MAGIC
# MAGIC      SELECT COUNT(*) FROM table1 AS t1
# MAGIC      LEFT JOIN table2 AS t2 ON t1.ID1=t2.ID1;
# MAGIC
# MAGIC **Right Join**
# MAGIC
# MAGIC      SELECT COUNT(*) FROM table1 AS t1
# MAGIC      RIGHT JOIN table2 AS t2 ON t1.ID1=t2.ID1;
# MAGIC
# MAGIC **Full Outer Join**
# MAGIC
# MAGIC      SELECT COUNT(*) FROM table1 AS t1
# MAGIC      FULL OUTER JOIN table2 AS t2 ON t1.ID1=t2.ID1;
# MAGIC
# MAGIC **Cross Join**
# MAGIC
# MAGIC      SELECT COUNT(*) FROM table1 AS t1
# MAGIC      CROSS JOIN table2 AS t2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 01**

# COMMAND ----------

# MAGIC %md
# MAGIC **How many rows will generate when apply `INNER JOIN, LEFT JOIN & RIGHT JOIN' for below tables?**
# MAGIC
# MAGIC      TABLE1   TABLE2
# MAGIC         1       1
# MAGIC         1       1 
# MAGIC                 1
# MAGIC
# MAGIC      INNER JOIN: ?
# MAGIC      LEFT JOIN : ?
# MAGIC      RIGHT JOIN: ?
# MAGIC      FULL JOIN: ?
# MAGIC      CROSS JOIN: ?

# COMMAND ----------

# DBTITLE 1,Create Table 1
# MAGIC %sql
# MAGIC CREATE TABLE tbl_1A(ID1 INT);
# MAGIC
# MAGIC INSERT INTO tbl_1A
# MAGIC VALUES
# MAGIC (1),
# MAGIC (1);
# MAGIC
# MAGIC SELECT * FROM tbl_1A;

# COMMAND ----------

# DBTITLE 1,Create Table 2
# MAGIC %sql
# MAGIC CREATE TABLE tbl_2A(ID2 INT);
# MAGIC
# MAGIC INSERT INTO tbl_2A
# MAGIC VALUES
# MAGIC (1),
# MAGIC (1),
# MAGIC (1);
# MAGIC
# MAGIC SELECT * FROM tbl_2A;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **INNER JOIN**
# MAGIC - All **matching records** from both the tables.
# MAGIC   - **matching records**: 2(1's) X 3(1's) = 6

# COMMAND ----------

# DBTITLE 1,INNER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_1A
# MAGIC INNER JOIN tbl_2A ON tbl_1A.ID1=tbl_2A.ID2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **LEFT JOIN**
# MAGIC - **matching records** and **non-matching** records from the **left table**.
# MAGIC   - **matching records**: 2(1's) X 3(1's) = 6
# MAGIC   - **non-matching records**: NA

# COMMAND ----------

# DBTITLE 1,LEFT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_1A
# MAGIC LEFT JOIN tbl_2A ON tbl_1A.ID1=tbl_2A.ID2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **RIGHT JOIN**
# MAGIC - **matching records** and **non-matching** records from the **right table**.
# MAGIC   - **matching records**: 2(1's) X 3(1's) = 6
# MAGIC   - **non-matching records**: NA

# COMMAND ----------

# DBTITLE 1,RIGHT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_1A
# MAGIC RIGHT JOIN tbl_2A ON tbl_1A.ID1=tbl_2A.ID2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **FULL JOIN / FULL OUTER JOIN**
# MAGIC
# MAGIC - **FULL JOIN** retrieves **all rows from both tables**, filling in **NULL** values where matches do not exist.
# MAGIC - The **FULL JOIN or FULL OUTER JOIN** in SQL is used to **retrieve all rows from both tables** involved in the join, regardless of whether there is a match between the rows in the two tables.

# COMMAND ----------

# MAGIC %md
# MAGIC - It combines the results of both a **LEFT JOIN and a RIGHT JOIN**.
# MAGIC - When there is **no match**, the result will include **NULLs** for the columns of the table that do not have a matching row.
# MAGIC
# MAGIC                                            (or)                                          
# MAGIC - **matching records (inner join)** and **non-matching records** of **left & right** tables.
# MAGIC   - **matching records**: 2(1's) X 3(1's) = 6
# MAGIC   - **non-matching records**: NA

# COMMAND ----------

# DBTITLE 1,FULL JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_1A
# MAGIC FULL OUTER JOIN tbl_2A ON tbl_1A.ID1=tbl_2A.ID2;

# COMMAND ----------

# DBTITLE 1,CROSS JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_1A
# MAGIC CROSS JOIN tbl_2A;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Example 01**
# MAGIC
# MAGIC **How many rows will generate when apply `INNER JOIN, LEFT JOIN & RIGHT JOIN` for below tables?**
# MAGIC
# MAGIC      TABLE1   TABLE2
# MAGIC         1       1
# MAGIC         1       1
# MAGIC         1       1
# MAGIC         2       1
# MAGIC                 3
# MAGIC
# MAGIC **INNER JOIN**
# MAGIC   - **matching records**: 3(1's) X 4(1's) = 12
# MAGIC      
# MAGIC **LEFT JOIN**
# MAGIC   - **matching records**: 3(1's) X 4(1's) = 12
# MAGIC   - **non-matching records of Left table**: 1(2's) = 1
# MAGIC
# MAGIC **RIGHT JOIN**
# MAGIC   - **matching records**: 3(1's) X 4(1's) = 12
# MAGIC   - **non-matching records of Right table**: 1(3's) = 1
# MAGIC
# MAGIC **FULL JOIN**
# MAGIC   - **matching records**: 3(1's) X 4(1's) = 12
# MAGIC   - **non-matching records of Left table**: 1(2's) = 1
# MAGIC   - **non-matching records of Right table**: 1(3's) = 1
# MAGIC
# MAGIC **CROSS JOIN**
# MAGIC   - 4 x 5 = 20

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 02**

# COMMAND ----------

# MAGIC %md
# MAGIC **How many rows will generate when apply `INNER JOIN, LEFT JOIN & RIGHT JOIN' for below tables?**
# MAGIC
# MAGIC      TAB1   TAB2
# MAGIC        1      1
# MAGIC        1      1
# MAGIC        1     NULL
# MAGIC
# MAGIC      INNER JOIN: ?
# MAGIC      LEFT JOIN : ?
# MAGIC      RIGHT JOIN: ?
# MAGIC      FULL JOIN: ?
# MAGIC      CROSS JOIN: ?

# COMMAND ----------

# DBTITLE 1,Create Table 1
# MAGIC %sql
# MAGIC CREATE TABLE TAB1(ID1 INT);
# MAGIC
# MAGIC INSERT INTO TAB1
# MAGIC VALUES
# MAGIC (1),
# MAGIC (1),
# MAGIC (1);
# MAGIC
# MAGIC SELECT * FROM TAB1;

# COMMAND ----------

# DBTITLE 1,Create Table 2
# MAGIC %sql
# MAGIC CREATE TABLE TAB2(ID1 INT);
# MAGIC
# MAGIC INSERT INTO TAB2
# MAGIC VALUES
# MAGIC (1),
# MAGIC (1),
# MAGIC (NULL);
# MAGIC
# MAGIC SELECT * FROM TAB2;

# COMMAND ----------

# MAGIC %md
# MAGIC - **In Databricks Community Edition, you cannot directly execute two SQL queries in the same %sql cell and display both outputs simultaneously.**
# MAGIC
# MAGIC       %sql
# MAGIC       SELECT * FROM TAB1
# MAGIC       INNER JOIN TAB2 ON TAB1.ID1=TAB2.ID1;
# MAGIC
# MAGIC       SELECT COUNT(*) FROM TAB1
# MAGIC       INNER JOIN TAB2 ON TAB1.ID1=TAB2.ID1;
# MAGIC
# MAGIC - **Option 01: Using Temporary Views**
# MAGIC
# MAGIC       -- Execute the first query and create a temporary view
# MAGIC       %sql
# MAGIC       CREATE OR REPLACE TEMP VIEW joined_table AS
# MAGIC       SELECT * FROM TAB1 
# MAGIC       INNER JOIN TAB2 ON TAB1.ID1 = TAB2.ID1;
# MAGIC
# MAGIC       -- Display the first query result
# MAGIC       %sql
# MAGIC       SELECT * FROM joined_table;
# MAGIC
# MAGIC       -- Display the count
# MAGIC       %sql
# MAGIC       SELECT COUNT(*) FROM joined_table;
# MAGIC
# MAGIC - **Option 02: Use Python to Combine Outputs**
# MAGIC
# MAGIC       # Execute the first query
# MAGIC       df1 = spark.sql("""
# MAGIC       SELECT * FROM TAB1 
# MAGIC       INNER JOIN TAB2 ON TAB1.ID1 = TAB2.ID1
# MAGIC       """)
# MAGIC
# MAGIC       # Execute the second query
# MAGIC       df2 = spark.sql("""
# MAGIC       SELECT COUNT(*) AS total_count FROM TAB1 
# MAGIC       INNER JOIN TAB2 ON TAB1.ID1 = TAB2.ID1
# MAGIC       """)
# MAGIC
# MAGIC       # Display both outputs
# MAGIC       print("First Query Output:")
# MAGIC       display(df1)
# MAGIC
# MAGIC       print("Second Query Output:")
# MAGIC       display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **INNER JOIN**
# MAGIC - An **inner join** returns rows with **matching values** in **both tables**.
# MAGIC - **matching records**:
# MAGIC   - 3(1's) X 2(1's) = 6

# COMMAND ----------

# DBTITLE 1,INNER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM TAB1
# MAGIC INNER JOIN TAB2 ON TAB1.ID1=TAB2.ID1;

# COMMAND ----------

# DBTITLE 1,Count after inner join
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM TAB1
# MAGIC INNER JOIN TAB2 ON TAB1.ID1=TAB2.ID1;

# COMMAND ----------

# Execute the first query
df1 = spark.sql("""
SELECT * FROM TAB1 
INNER JOIN TAB2 ON TAB1.ID1 = TAB2.ID1
""")

# Execute the second query
df2 = spark.sql("""
SELECT COUNT(*) AS total_count FROM TAB1 
INNER JOIN TAB2 ON TAB1.ID1 = TAB2.ID1
""")

# Display both outputs
print("First Query Output:")
display(df1)

print("Second Query Output:")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **LEFT JOIN**
# MAGIC - A **left join** returns **all rows** from the **left table** and **matched rows** from the **right table**. **Unmatched rows** from the **right table** result in **NULLs**.
# MAGIC
# MAGIC                                       (or)
# MAGIC - **All records** from **left** table. Only **matching records** from **right** table.
# MAGIC
# MAGIC                                       (or)
# MAGIC - **matching records** and **non-matching** records from the **left table**.
# MAGIC   - **matching records**:
# MAGIC     - 3(1's) X 2(1's) = 6
# MAGIC   - **non-matching records**:
# MAGIC     - NA

# COMMAND ----------

# DBTITLE 1,LEFT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM TAB1
# MAGIC LEFT JOIN TAB2 ON TAB1.ID1=TAB2.ID1;

# COMMAND ----------

# MAGIC %md
# MAGIC **RIGHT JOIN**
# MAGIC - A **right join** returns **all rows** from the **right table** and **matched rows** from the **left table**. **Unmatched rows** from the **left table** result in **NULLs**.
# MAGIC
# MAGIC                         (or)
# MAGIC - **All records** from **right** table. Only **matching records** from **left** table.
# MAGIC
# MAGIC                         (or)
# MAGIC - **matching records** and **non-matching** records from the **right table**.
# MAGIC   - **matching records**:
# MAGIC     - 3(1's) X 2(1's) = 6
# MAGIC   - **non-matching records**:
# MAGIC     - 1 (NULL) = 1 --> (NULL NULL)

# COMMAND ----------

# DBTITLE 1,RIGHT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM TAB1
# MAGIC RIGHT JOIN TAB2 ON TAB1.ID1=TAB2.ID1;

# COMMAND ----------

# MAGIC %md
# MAGIC **FULL JOIN / FULL OUTER JOIN**
# MAGIC - A **full outer join** returns **all rows** when there's a **match in either table**. **Unmatched rows** from **both tables** result in **NULLs**.
# MAGIC
# MAGIC                                          (or)
# MAGIC - All records from **ALL** tables.
# MAGIC - **matching records**:
# MAGIC     - 3(1's) X 2(1's) = 6
# MAGIC
# MAGIC - **non-matching records of Left table**:
# MAGIC     - NA
# MAGIC
# MAGIC - **non-matching records of Right table**:
# MAGIC     - 1 (NULL) = 1 --> (NULL NULL)

# COMMAND ----------

# DBTITLE 1,FULL JOIN
# MAGIC %sql
# MAGIC SELECT * FROM TAB1
# MAGIC FULL OUTER JOIN TAB2 ON TAB1.ID1=TAB2.ID1;

# COMMAND ----------

# MAGIC %md
# MAGIC **CROSS JOIN**
# MAGIC - A **cross join** returns the **Cartesian product of both tables**, combining **each row from the first table with every row from the second table**.
# MAGIC - 3 x 3 = 9

# COMMAND ----------

# DBTITLE 1,CROSS JOIN
# MAGIC %sql
# MAGIC SELECT * FROM TAB1
# MAGIC CROSS JOIN TAB2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 03**

# COMMAND ----------

# MAGIC %md
# MAGIC **What is the output if we apply inner join?**
# MAGIC
# MAGIC       TABLE1   TABLE2
# MAGIC          1      1
# MAGIC          1      2
# MAGIC          2      1
# MAGIC          1      1

# COMMAND ----------

# DBTITLE 1,Create Table tbl_q2_01
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_q2_01;
# MAGIC
# MAGIC CREATE TABLE tbl_q2_01(ID1 INT);
# MAGIC
# MAGIC INSERT INTO tbl_q2_01
# MAGIC VALUES
# MAGIC (1),
# MAGIC (1),
# MAGIC (2),
# MAGIC (1);
# MAGIC
# MAGIC SELECT * FROM tbl_q2_01;

# COMMAND ----------

# DBTITLE 1,Create Table tbl_q2_02
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_q2_02;
# MAGIC
# MAGIC CREATE TABLE tbl_q2_02(ID2 INT);
# MAGIC
# MAGIC  INSERT INTO tbl_q2_02
# MAGIC  VALUES
# MAGIC  (1),
# MAGIC  (2),
# MAGIC  (1),
# MAGIC  (1);
# MAGIC
# MAGIC SELECT * FROM tbl_q2_02;

# COMMAND ----------

# MAGIC %md
# MAGIC **INNER JOIN**
# MAGIC - An **inner join** returns rows with **matching values** in **both tables**.
# MAGIC - **matching records**:
# MAGIC   - 3(1's) X 3(1's) = 9
# MAGIC   - 1(2's) X 1(2's) = 1

# COMMAND ----------

# DBTITLE 1,INNER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_q2_01
# MAGIC INNER JOIN tbl_q2_02 ON tbl_q2_01.ID1=tbl_q2_02.ID2;

# COMMAND ----------

# MAGIC %md
# MAGIC **LEFT JOIN**
# MAGIC
# MAGIC - **matching records** and **non-matching** records from the **left table**.
# MAGIC   - **matching records**:
# MAGIC     - 3(1's) X 3(1's) = 9
# MAGIC     - 1(2's) X 1(2's) = 1
# MAGIC   - **non-matching records**:
# MAGIC     - NA

# COMMAND ----------

# DBTITLE 1,LEFT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_q2_01
# MAGIC LEFT JOIN tbl_q2_02 ON tbl_q2_01.ID1=tbl_q2_02.ID2;

# COMMAND ----------

# MAGIC %md
# MAGIC **RIGHT JOIN**
# MAGIC - **matching records** and **non-matching** records from the **right table**.
# MAGIC   - **matching records**:
# MAGIC     - 3(1's) X 3(1's) = 9
# MAGIC     - 1(2's) X 1(2's) = 1
# MAGIC   - **non-matching records**:
# MAGIC     - NA

# COMMAND ----------

# DBTITLE 1,RIGHT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_q2_01
# MAGIC RIGHT JOIN tbl_q2_02 ON tbl_q2_01.ID1=tbl_q2_02.ID2;

# COMMAND ----------

# MAGIC %md
# MAGIC **FULL JOIN / FULL OUTER JOIN**
# MAGIC
# MAGIC - **matching records**:
# MAGIC     - 3(1's) X 3(1's) = 9
# MAGIC     - 1(2's) X 1(2's) = 1
# MAGIC
# MAGIC - **non-matching records of Left table**:
# MAGIC     - NA
# MAGIC
# MAGIC - **non-matching records of Right table**:
# MAGIC     - NA

# COMMAND ----------

# DBTITLE 1,FULL JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_q2_01
# MAGIC FULL OUTER JOIN tbl_q2_02 ON tbl_q2_01.ID1=tbl_q2_02.ID2;

# COMMAND ----------

# MAGIC %md
# MAGIC **CROSS JOIN**
# MAGIC - A **cross join** returns the **Cartesian product of both tables**, combining **each row from the first table with every row from the second table**.
# MAGIC - 4 x 4 = 16

# COMMAND ----------

# DBTITLE 1,CROSS JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_q2_01
# MAGIC CROSS JOIN tbl_q2_02;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 04**

# COMMAND ----------

# MAGIC %md
# MAGIC      TABLE1   TABLE2
# MAGIC        1        1
# MAGIC        1        2
# MAGIC        2        3
# MAGIC        3        2
# MAGIC        4        6
# MAGIC        3

# COMMAND ----------

# DBTITLE 1,Create Table tbl_AAZ
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_AAZ;
# MAGIC
# MAGIC CREATE TABLE tbl_AAZ(ID INT);
# MAGIC
# MAGIC INSERT INTO tbl_AAZ
# MAGIC VALUES
# MAGIC (1),
# MAGIC (1),
# MAGIC (2),
# MAGIC (3),
# MAGIC (4),
# MAGIC (3);
# MAGIC
# MAGIC SELECT * FROM tbl_AAZ;

# COMMAND ----------

# DBTITLE 1,Create Table tbl_BB
# MAGIC %sql
# MAGIC  DROP TABLE IF EXISTS tbl_BBZ;
# MAGIC
# MAGIC  CREATE TABLE tbl_BBZ(ID INT);
# MAGIC
# MAGIC  INSERT INTO tbl_BBZ
# MAGIC  VALUES
# MAGIC  (1),
# MAGIC  (2),
# MAGIC  (3),
# MAGIC  (2),
# MAGIC  (6);
# MAGIC
# MAGIC  SELECT * FROM tbl_BBZ;

# COMMAND ----------

# MAGIC %md
# MAGIC **INNER JOIN**
# MAGIC - An **inner join** returns rows with **matching values** in **both tables**.
# MAGIC - **matching records**:
# MAGIC   - 2(1's) X 1(1's) = 2
# MAGIC   - 1(2's) X 2(2's) = 2
# MAGIC   - 2(3's) X 1(3's) = 2

# COMMAND ----------

# DBTITLE 1,INNER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AAZ
# MAGIC INNER JOIN tbl_BBZ
# MAGIC ON tbl_AAZ.ID = tbl_BBZ.ID

# COMMAND ----------

# MAGIC %md
# MAGIC **LEFT JOIN**
# MAGIC
# MAGIC - **matching records** and **non-matching** records from the **left table**.
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 1(1's) = 2
# MAGIC     - 1(2's) X 2(2's) = 2
# MAGIC     - 2(3's) X 1(3's) = 2
# MAGIC   - **non-matching records**:
# MAGIC     - 1(4's) = 1 --> ( 4 NULL)

# COMMAND ----------

# DBTITLE 1,LEFT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AAZ
# MAGIC LEFT JOIN tbl_BBZ
# MAGIC ON tbl_AAZ.ID = tbl_BBZ.ID

# COMMAND ----------

# MAGIC %md
# MAGIC **RIGHT JOIN**
# MAGIC
# MAGIC - **matching records** and **non-matching** records from the **right table**.
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 1(1's) = 2
# MAGIC     - 1(2's) X 2(2's) = 2
# MAGIC     - 2(3's) X 1(3's) = 2
# MAGIC   - **non-matching records**:
# MAGIC     - 1(6's) = 1 --> (NULL 6)

# COMMAND ----------

# DBTITLE 1,RIGHT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AAZ
# MAGIC RIGHT JOIN tbl_BBZ
# MAGIC ON tbl_AAZ.ID = tbl_BBZ.ID

# COMMAND ----------

# MAGIC %md
# MAGIC **FULL JOIN / FULL OUTER JOIN**
# MAGIC
# MAGIC - **matching records**:
# MAGIC     - 2(1's) X 1(1's) = 2
# MAGIC     - 1(2's) X 2(2's) = 2
# MAGIC     - 2(3's) X 1(3's) = 2
# MAGIC
# MAGIC - **non-matching records of Left table**:
# MAGIC     - 1(4's) = 1 --> ( 4 NULL)
# MAGIC
# MAGIC - **non-matching records of Right table**:
# MAGIC     - 1(6's) = 1 --> (NULL 6)

# COMMAND ----------

# DBTITLE 1,FULL JOIN
# MAGIC %sql
# MAGIC  SELECT * FROM tbl_AAZ
# MAGIC  FULL OUTER JOIN tbl_BBZ
# MAGIC  ON tbl_AAZ.ID = tbl_BBZ.ID

# COMMAND ----------

# MAGIC %md
# MAGIC **CROSS JOIN**
# MAGIC - A **cross join** returns the **Cartesian product of both tables**, combining **each row from the first table with every row from the second table**.
# MAGIC - 6 x 5 = 30

# COMMAND ----------

# DBTITLE 1,CROSS JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AAZ
# MAGIC CROSS JOIN tbl_BBZ;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 05**

# COMMAND ----------

# MAGIC %md
# MAGIC **How many rows will generate when apply `INNER JOIN, LEFT JOIN, RIGHT JOIN & FULL JOIN` for below tables?**
# MAGIC
# MAGIC      TABLE1   TABLE2
# MAGIC        1        1
# MAGIC        1        2
# MAGIC        2        3
# MAGIC        3        2
# MAGIC        4       NULL
# MAGIC        3
# MAGIC      NULL
# MAGIC      NULL

# COMMAND ----------

# DBTITLE 1,Create Table tbl_AAA
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_AAA;
# MAGIC
# MAGIC CREATE TABLE tbl_AAA(ID INT);
# MAGIC
# MAGIC INSERT INTO tbl_AAA
# MAGIC VALUES
# MAGIC (1),
# MAGIC (1),
# MAGIC (2),
# MAGIC (3),
# MAGIC (4),
# MAGIC (3),
# MAGIC (NULL),
# MAGIC (NULL);
# MAGIC
# MAGIC SELECT * FROM tbl_AAA;

# COMMAND ----------

# dbutils.fs.rm('dbfs:/user/hive/warehouse/tbl_aaa', True)
# dbutils.fs.rm('dbfs:/user/hive/warehouse/tbl_bbb', True)

# COMMAND ----------

# DBTITLE 1,Create Table tbl_BBB
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_BBB;
# MAGIC
# MAGIC CREATE TABLE tbl_BBB(ID INT);
# MAGIC
# MAGIC INSERT INTO tbl_BBB
# MAGIC VALUES
# MAGIC (1),
# MAGIC (2),
# MAGIC (3),
# MAGIC (2),
# MAGIC (NULL);
# MAGIC
# MAGIC SELECT * FROM tbl_BBB;

# COMMAND ----------

# MAGIC %md
# MAGIC **INNER JOIN**
# MAGIC - An **inner join** returns rows with **matching values** in **both tables**.
# MAGIC - **matching records**:
# MAGIC   - 2(1's) X 1(1's) = 2
# MAGIC   - 1(2's) X 2(2's) = 2
# MAGIC   - 2(3's) X 1(3's) = 2

# COMMAND ----------

# DBTITLE 1,INNER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AAA
# MAGIC INNER JOIN tbl_BBB
# MAGIC ON tbl_AAA.ID = tbl_BBB.ID

# COMMAND ----------

# MAGIC %md
# MAGIC **LEFT JOIN**
# MAGIC
# MAGIC - **matching records** and **non-matching** records from the **left table**.
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 1(1's) = 2
# MAGIC     - 1(2's) X 2(2's) = 2
# MAGIC     - 2(3's) X 1(3's) = 2
# MAGIC   - **non-matching records**:
# MAGIC     - 1(4's) = 1 --> ( 4 NULL)
# MAGIC     - 1(NULL) = 1 --> (NULL NULL)
# MAGIC     - 1(NULL) = 1 --> (NULL NULL)

# COMMAND ----------

# DBTITLE 1,LEFT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AAA
# MAGIC LEFT JOIN tbl_BBB
# MAGIC ON tbl_AAA.ID = tbl_BBB.ID

# COMMAND ----------

# MAGIC %md
# MAGIC **RIGHT JOIN**
# MAGIC
# MAGIC - **matching records** and **non-matching** records from the **right table**.
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 1(1's) = 2
# MAGIC     - 1(2's) X 2(2's) = 2
# MAGIC     - 2(3's) X 1(3's) = 2
# MAGIC   - **non-matching records**:
# MAGIC     - 1(NULL) = 1 --> (NULL NULL)

# COMMAND ----------

# DBTITLE 1,RIGHT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AAA
# MAGIC RIGHT JOIN tbl_BBB
# MAGIC ON tbl_AAA.ID = tbl_BBB.ID

# COMMAND ----------

# MAGIC %md
# MAGIC **FULL JOIN / FULL OUTER JOIN**
# MAGIC
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 1(1's) = 2
# MAGIC     - 1(2's) X 2(2's) = 2
# MAGIC     - 2(3's) X 1(3's) = 2
# MAGIC   - **non-matching records of Left table**:
# MAGIC     - 1(4's) = 1 --> ( 4 NULL)
# MAGIC     - 1(NULL) = 1 --> (NULL NULL)
# MAGIC     - 1(NULL) = 1 --> (NULL NULL)
# MAGIC
# MAGIC - **non-matching records of Right table**:
# MAGIC     - 1(NULL) = 1 --> (NULL NULL)

# COMMAND ----------

# DBTITLE 1,FULL JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AAA
# MAGIC FULL OUTER JOIN tbl_BBB
# MAGIC ON tbl_AAA.ID = tbl_BBB.ID

# COMMAND ----------

# MAGIC %md
# MAGIC - A **cross join** returns the **Cartesian product of both tables**, combining **each row from the first table with every row from the second table**.
# MAGIC - 8 x 5 = 40

# COMMAND ----------

# DBTITLE 1,CROSS JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AAA
# MAGIC CROSS JOIN tbl_BBB;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 06**

# COMMAND ----------

# MAGIC %md
# MAGIC **Inner Join**
# MAGIC
# MAGIC      table1  table2
# MAGIC        2       2
# MAGIC                2
# MAGIC                2
# MAGIC                2

# COMMAND ----------

# DBTITLE 1,Create Table tbl_A1
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_A1;
# MAGIC
# MAGIC CREATE TABLE tbl_A1(ID INT);
# MAGIC
# MAGIC INSERT INTO tbl_A1
# MAGIC VALUES
# MAGIC (2);
# MAGIC
# MAGIC SELECT * FROM tbl_A1;

# COMMAND ----------

# DBTITLE 1,Create Table tbl_A2
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_A2;
# MAGIC
# MAGIC CREATE TABLE tbl_A2(ID INT);
# MAGIC
# MAGIC INSERT INTO tbl_A2
# MAGIC VALUES
# MAGIC (2),
# MAGIC (2),
# MAGIC (2),
# MAGIC (2);
# MAGIC
# MAGIC SELECT * FROM tbl_A2;

# COMMAND ----------

# MAGIC %md
# MAGIC **INNER JOIN**
# MAGIC - An **inner join** returns rows with **matching values** in **both tables**.
# MAGIC - **matching records**:
# MAGIC   - 1(1's) X 4(1's) = 4

# COMMAND ----------

# DBTITLE 1,INNER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_A1
# MAGIC INNER JOIN tbl_A2 ON tbl_A1.ID = tbl_A2.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **CROSS JOIN**
# MAGIC - A **cross join** returns the **Cartesian product of both tables**, combining **each row from the first table with every row from the second table**.
# MAGIC - 1 x 4 = 4

# COMMAND ----------

# DBTITLE 1,CROSS JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_A1
# MAGIC CROSS JOIN tbl_A2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 07**

# COMMAND ----------

# MAGIC %md
# MAGIC      TABLE1   TABLE2
# MAGIC        1         1
# MAGIC        1         1
# MAGIC                  1

# COMMAND ----------

# DBTITLE 1,Create Table tbl_AAA1
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_AAA1;
# MAGIC CREATE TABLE tbl_AAA1(ID INT);
# MAGIC
# MAGIC INSERT INTO tbl_AAA1
# MAGIC VALUES
# MAGIC (1),
# MAGIC (1);
# MAGIC
# MAGIC SELECT * FROM tbl_AAA1;

# COMMAND ----------

# DBTITLE 1,Create Table tbl_AAA2
# MAGIC %sql
# MAGIC  DROP TABLE IF EXISTS tbl_AAA2;
# MAGIC  CREATE TABLE tbl_AAA2(ID INT);
# MAGIC
# MAGIC  INSERT INTO tbl_AAA2
# MAGIC  VALUES
# MAGIC  (1),
# MAGIC  (1),
# MAGIC  (1);
# MAGIC
# MAGIC  SELECT * FROM tbl_AAA2;

# COMMAND ----------

# MAGIC %md
# MAGIC - Same output for all joins **(Inner Join, Left Join, Right Join and Full Outer Join)**

# COMMAND ----------

# MAGIC %md
# MAGIC **INNER JOIN**
# MAGIC - An **inner join** returns rows with **matching values** in **both tables**.
# MAGIC - **matching records**:
# MAGIC   - 2(1's) X 3(1's) = 6
# MAGIC
# MAGIC **LEFT JOIN**
# MAGIC - **matching records** and **non-matching** records from the **left table**.
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 3(1's) = 6
# MAGIC   - **non-matching records**:
# MAGIC     - NA
# MAGIC
# MAGIC **RIGHT JOIN**
# MAGIC - **matching records** and **non-matching** records from the **right table**.
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 3(1's) = 6
# MAGIC   - **non-matching records**:
# MAGIC     - NA
# MAGIC
# MAGIC **FULL JOIN / FULL OUTER JOIN**
# MAGIC - **matching records**:
# MAGIC     - 2(1's) X 3(1's) = 6
# MAGIC - **non-matching records of Left table**:
# MAGIC     - NA
# MAGIC - **non-matching records of Right table**:
# MAGIC     - NA
# MAGIC
# MAGIC **CROSS JOIN**
# MAGIC - A **cross join** returns the **Cartesian product of both tables**, combining **each row from the first table with every row from the second table**.
# MAGIC - 2 x 3 = 6

# COMMAND ----------

# DBTITLE 1,INNER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_AAA1
# MAGIC INNER JOIN tbl_AAA2 ON tbl_AAA1.ID = tbl_AAA2.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 8**

# COMMAND ----------

# MAGIC %md
# MAGIC      TABLE1  TABLE2
# MAGIC        1        1
# MAGIC        1        1
# MAGIC        2        1
# MAGIC                 3

# COMMAND ----------

# DBTITLE 1,Create Table tbl_C1
# MAGIC %sql
# MAGIC  DROP TABLE IF EXISTS tbl_C1;
# MAGIC  
# MAGIC  CREATE TABLE tbl_C1(ID INT);
# MAGIC
# MAGIC  INSERT INTO tbl_C1
# MAGIC  VALUES
# MAGIC  (1),
# MAGIC  (1),
# MAGIC  (2);
# MAGIC
# MAGIC  SELECT * FROM tbl_C1;

# COMMAND ----------

# DBTITLE 1,Create Table tbl_C2
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_C2;
# MAGIC
# MAGIC CREATE TABLE tbl_C2(ID INT);
# MAGIC
# MAGIC INSERT INTO tbl_C2
# MAGIC VALUES
# MAGIC (1),
# MAGIC (1),
# MAGIC (1),
# MAGIC (3);
# MAGIC
# MAGIC SELECT * FROM tbl_C2;

# COMMAND ----------

# MAGIC %md
# MAGIC **INNER JOIN**
# MAGIC - An **inner join** returns rows with **matching values** in **both tables**.
# MAGIC - **matching records**:
# MAGIC   - 2(1's) X 3(1's) = 6

# COMMAND ----------

# DBTITLE 1,INNER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_C1
# MAGIC INNER JOIN tbl_C2 ON tbl_C1.ID = tbl_C2.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **LEFT JOIN**
# MAGIC - **matching records** and **non-matching** records from the **left table**.
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 3(1's) = 6
# MAGIC   - **non-matching records**:
# MAGIC     - 1(2's) = 1 --> (2 NULL)

# COMMAND ----------

# DBTITLE 1,LEFT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_C1
# MAGIC LEFT JOIN tbl_C2 ON tbl_C1.ID = tbl_C2.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **RIGHT JOIN**
# MAGIC - **matching records** and **non-matching** records from the **right table**.
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 3(1's) = 6
# MAGIC   - **non-matching records**:
# MAGIC     - 1(3's) = 1 --> (NULL 3)

# COMMAND ----------

# DBTITLE 1,RIGHT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_C1
# MAGIC RIGHT JOIN tbl_C2 ON tbl_C1.ID = tbl_C2.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **FULL JOIN / FULL OUTER JOIN**
# MAGIC - **matching records**:
# MAGIC     - 2(1's) X 3(1's) = 6
# MAGIC
# MAGIC - **non-matching records of Left table**:
# MAGIC     - 1(2's) = 1 --> (2 NULL)
# MAGIC
# MAGIC - **non-matching records of Right table**:
# MAGIC     - 1(3's) = 1 --> (NULL 3)

# COMMAND ----------

# DBTITLE 1,FULL JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_C1
# MAGIC FULL OUTER JOIN tbl_C2 ON tbl_C1.ID = tbl_C2.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC - A **cross join** returns the **Cartesian product of both tables**, combining **each row from the first table with every row from the second table**.
# MAGIC - 3 x 4 = 12

# COMMAND ----------

# DBTITLE 1,CROSS JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_C1
# MAGIC CROSS JOIN tbl_C2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Question 9**

# COMMAND ----------

# MAGIC %md
# MAGIC      TABLE1    TABLE2
# MAGIC         1        1
# MAGIC         1        1
# MAGIC         2        1
# MAGIC         2        3
# MAGIC                  2

# COMMAND ----------

# DBTITLE 1,Create Table tbl_CC1
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tbl_CC1;
# MAGIC
# MAGIC CREATE TABLE tbl_CC1(ID INT);
# MAGIC
# MAGIC INSERT INTO tbl_CC1
# MAGIC VALUES
# MAGIC (1),
# MAGIC (1),
# MAGIC (2),
# MAGIC (2);
# MAGIC
# MAGIC SELECT * FROM tbl_CC1;

# COMMAND ----------

# dbutils.fs.rm('dbfs:/user/hive/warehouse/tbl_cc1', True)
# dbutils.fs.rm('dbfs:/user/hive/warehouse/tbl_cc2', True)

# COMMAND ----------

# DBTITLE 1,Create Table tbl_CC2
# MAGIC %sql
# MAGIC  DROP TABLE IF EXISTS tbl_CC2;
# MAGIC
# MAGIC  CREATE TABLE tbl_CC2(ID INT);
# MAGIC
# MAGIC  INSERT INTO tbl_CC2
# MAGIC  VALUES
# MAGIC  (1),
# MAGIC  (1),
# MAGIC  (1),
# MAGIC  (3),
# MAGIC  (2);
# MAGIC
# MAGIC  SELECT * FROM tbl_CC2;

# COMMAND ----------

# MAGIC %md
# MAGIC **INNER JOIN**
# MAGIC - An **inner join** returns rows with **matching values** in **both tables**.
# MAGIC - **matching records**:
# MAGIC   - 2(1's) X 3(1's) = 6
# MAGIC   - 2(2's) X 1(2's) = 2

# COMMAND ----------

# DBTITLE 1,INNER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_CC1
# MAGIC INNER JOIN tbl_CC2 ON tbl_CC1.ID = tbl_CC2.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **LEFT JOIN**
# MAGIC - **matching records** and **non-matching** records from the **left table**.
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 3(1's) = 6
# MAGIC     - 2(2's) X 1(2's) = 2
# MAGIC   - **non-matching records**:
# MAGIC     - NA

# COMMAND ----------

# DBTITLE 1,LEFT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_CC1
# MAGIC LEFT JOIN tbl_CC2 ON tbl_CC1.ID = tbl_CC2.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **RIGHT JOIN**
# MAGIC - **matching records** and **non-matching** records from the **right table**.
# MAGIC   - **matching records**:
# MAGIC     - 2(1's) X 3(1's) = 6
# MAGIC     - 2(2's) X 1(2's) = 2
# MAGIC   - **non-matching records**:
# MAGIC     - 1(3's)= 1 --> (NULL 3)

# COMMAND ----------

# DBTITLE 1,RIGHT JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_CC1
# MAGIC RIGHT JOIN tbl_CC2 ON tbl_CC1.ID = tbl_CC2.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **FULL JOIN / FULL OUTER JOIN**
# MAGIC - **matching records**:
# MAGIC     - 2(1's) X 3(1's) = 6
# MAGIC     - 2(2's) X 1(2's) = 2
# MAGIC - **non-matching records of Left table**:
# MAGIC     - NA
# MAGIC - **non-matching records of Right table**:
# MAGIC     - 1(3's)= 1 --> (NULL 3)

# COMMAND ----------

# DBTITLE 1,FULL JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_CC1
# MAGIC FULL OUTER JOIN tbl_CC2 ON tbl_CC1.ID = tbl_CC2.ID;

# COMMAND ----------

# MAGIC %md
# MAGIC **CROSS JOIN**
# MAGIC - A **cross join** returns the **Cartesian product of both tables**, combining **each row from the first table with every row from the second table**.
# MAGIC - 4 x 5 = 20

# COMMAND ----------

# DBTITLE 1,CROSS JOIN
# MAGIC %sql
# MAGIC SELECT * FROM tbl_CC1
# MAGIC CROSS JOIN tbl_CC2;
